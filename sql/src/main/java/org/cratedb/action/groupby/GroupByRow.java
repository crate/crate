package org.cratedb.action.groupby;

import com.google.common.base.Joiner;
import org.cratedb.DataType;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggState;
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
        for (int i = 0; i < seenValuesList.size(); i++) {
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
        int idx = 0;
        for (Set<Object> seenValue : seenValuesList) {
            seenValue.addAll(otherRow.seenValuesList.get(idx));
            idx++;
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
        Set<Object> values;
        int valuesSize;
        int seenValuesSize = in.readVInt();
        seenValuesList = new ArrayList<>(seenValuesSize);
        for (int i = 0; i < seenValuesSize; i++) {
            valuesSize = in.readVInt();
            values = new HashSet<>(valuesSize);
            for (int j = 0; j < valuesSize; j++) {
                values.add(in.readGenericValue());
            }
            seenValuesList.add(values);
        }

        aggStates = new ArrayList<>(stmt.aggregateExpressions().size());
        AggExpr aggExpr;
        int seenIdxIndex = 0;
        for (int i = 0; i < aggStates.size(); i++) {
            aggExpr = stmt.aggregateExpressions().get(i);
            aggStates.add(i, aggExpr.createAggState());
            aggStates.get(i).readFrom(in);
            if (aggExpr.isDistinct) {
                aggStates.get(i).setSeenValuesRef(seenValuesList.get(stmt.seenIdxMap().get
                        (seenIdxIndex++)));
            }
        }
    }


    public void writeTo(DataType.Streamer[] keyStreamers, StreamOutput out) throws IOException {
        for (int i = 0; i < keyStreamers.length; i++) {
            keyStreamers[i].writeTo(out, key.get(i));
        }
        writeStates(out);
    }

    public void writeStates(StreamOutput out) throws IOException {

        out.writeVInt(seenValuesList.size());
        for (Set<Object> seenValue : seenValuesList) {
            out.writeVInt(seenValue.size());
            for (Object o : seenValue) {
                out.writeGenericValue(o);
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
