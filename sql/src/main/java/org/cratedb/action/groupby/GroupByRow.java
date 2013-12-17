package org.cratedb.action.groupby;

import com.google.common.base.Joiner;
import org.cratedb.DataType;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggState;
import org.cratedb.action.groupby.aggregate.min.MinAggState;
import org.cratedb.action.sql.ParsedStatement;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.*;

/**
 * Represents the resulting row of a "select with group by" statement.
 *
 * Assuming a query as follows:
 *
 *      select count(*), x, avg(*), y from... group by x, y
 *
 * Then the data is structured in the following form:
 *
 *  AggegrateStates: [ CountAggState, AvgAggState ]
 *  GroupByKey: (Object[]) { "ValueX", "ValueY" }
 */
public class GroupByRow {

    public List<Set<Object>> seenValuesList;
    public GroupByKey key;

    public List<AggState> aggStates;
    public boolean[] continueCollectingFlags;  // not serialized, only for collecting

    public GroupByRow() {
    }

    public GroupByRow(GroupByKey key, List<AggState> aggStates,
                      ParsedStatement stmt, List<Set<Object>> seenValuesList)
    {
        this.key = key;
        this.aggStates = aggStates;
        this.seenValuesList = seenValuesList;
        this.continueCollectingFlags = new boolean[aggStates != null ? aggStates.size() : 0];
        Arrays.fill(this.continueCollectingFlags, true);
    }

    /**
     * use this ctor only for testing as serialization won't work because the aggExpr and seenValues are missing!
     */
    public GroupByRow(GroupByKey key, List<AggState> aggStates, ParsedStatement stmt) {
        this(key, aggStates, stmt, new ArrayList<Set<Object>>(0));
    }

    public static GroupByRow createEmptyRow(GroupByKey key, ParsedStatement stmt) {
        List<Set<Object>> seenValuesList = new ArrayList<>(stmt.seenIdxMap().size());
        for (int i = 0; i < stmt.seenIdxMap().size(); i++) {
            // TODO: we should use specific sets here for performance
            seenValuesList.add(new HashSet<>());
        }

        List<AggState> aggStates = new ArrayList<>(stmt.aggregateExpressions().size());
        AggState aggState;

        if (stmt.seenIdxMap().size() > 0) {
            int idx = 0;
            for (AggExpr aggExpr : stmt.aggregateExpressions()) {
                aggState = aggExpr.createAggState();
                if (aggExpr.isDistinct) {
                    aggState.setSeenValuesRef(seenValuesList.get(stmt.seenIdxMap().get(idx)));
                    idx++;
                }
                aggStates.add(aggState);
            }
        } else {
            for (AggExpr aggExpr : stmt.aggregateExpressions()) {
                aggStates.add(aggExpr.createAggState());
            }
        }

        return new GroupByRow(key, aggStates, stmt, seenValuesList);
    }

    @Override
    public String toString() {
        return "GroupByRow{" +
            "aggregateStates=" + Joiner.on(", ").join(aggStates) +
            ", key=" + key +
            '}';
    }

    public int size() {
        return key.size() + aggStates.size();
    }

    @SuppressWarnings("unchecked")
    public synchronized void merge(GroupByRow otherRow) {
        // merge is done before-hand so that the aggStates don't have to in their reduce method.
        // this avoids doing the merge twice if aggStates share the seenValues.
        if (seenValuesList != null && seenValuesList.size()>0){
            int idx = 0;
            for (Set<Object> seenValue : seenValuesList) {
                seenValue.addAll(otherRow.seenValuesList.get(idx++));
            }
        }
        for (int i = 0; i < aggStates.size(); i++) {
            aggStates.get(i).reduce(otherRow.aggStates.get(i));
        }
    }


    public static GroupByRow readGroupByRow(ParsedStatement stmt,
            DataType.Streamer[] keyStreamers,
            StreamInput in) throws IOException
    {
        GroupByRow row = new GroupByRow();
        Object[] keyValue = new Object[keyStreamers.length];
        for (int i = 0; i < keyValue.length; i++) {
            keyValue[i] = keyStreamers[i].readFrom(in);
        }
        row.readFrom(in, new GroupByKey(keyValue), stmt);
        return row;
    }

    public void readFrom(StreamInput in, GroupByKey key, ParsedStatement stmt) throws
            IOException {
        this.key = key;
        readStates(in, stmt);
    }

    public void readStates(StreamInput in, ParsedStatement stmt) throws IOException {

        DataType.Streamer[] streamers = stmt.getSeenValueStreamers();
        if (streamers != null && streamers.length>0){
            seenValuesList = new ArrayList<>(streamers.length);
            for (DataType.Streamer streamer: streamers){
                int length = in.readVInt();
                Set<Object> seenValue = new HashSet<>(length);
                for (int i = 0; i < length; i++) {
                    seenValue.add(streamer.readFrom(in));
                }
                seenValuesList.add(seenValue);
            }
        }

        aggStates = new ArrayList<>(stmt.aggregateExpressions().size());
        AggExpr aggExpr;
        int seenIdxIndex = 0;
        for (int i = 0; i < stmt.aggregateExpressions().size(); i++) {
            aggExpr = stmt.aggregateExpressions().get(i);
            aggStates.add(i, aggExpr.createAggState());
            aggStates.get(i).readFrom(in);
            if (aggExpr.isDistinct) {
                aggStates.get(i).setSeenValuesRef(seenValuesList.get(stmt.seenIdxMap().get(seenIdxIndex++)));
            }
        }
    }


    public void writeTo(DataType.Streamer[] keyStreamers, ParsedStatement stmt, StreamOutput out)
            throws IOException {
        for (int i = 0; i < keyStreamers.length; i++) {
            keyStreamers[i].writeTo(out, key.get(i));
        }
        writeStates(out, stmt);
    }

    public void writeStates(StreamOutput out, ParsedStatement stmt) throws IOException {

        DataType.Streamer[] streamers = stmt.getSeenValueStreamers();
        if (streamers != null && streamers.length>0){
            int idx = 0;
            for (DataType.Streamer streamer: stmt.getSeenValueStreamers()){
                Set<Object> seenValue = seenValuesList.get(idx++);
                if (seenValue==null ||seenValue.size()==0){
                    out.writeVInt(0);
                    continue;
                }
                out.writeVInt(seenValue.size());
                for (Object v: seenValue){
                    streamer.writeTo(out, v);
                }
            }
        }

        for (AggState aggState : aggStates) {
            aggState.writeTo(out);
        }
    }


    public void terminatePartial() {
        for (AggState aggState : aggStates) {
            aggState.terminatePartial();
        }
    }
}
