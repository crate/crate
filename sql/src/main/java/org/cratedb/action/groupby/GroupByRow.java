package org.cratedb.action.groupby;

import com.google.common.base.Joiner;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

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
public class GroupByRow implements Streamable {

    public List<Set<Object>> seenValuesList;
    public GroupByKey key;

    public List<AggState> aggStates;
    public boolean[] continueCollectingFlags;  // not serialized, only for collecting

    public List<AggExpr> aggExprs;
    private List<Integer> seenIdxMapping;

    GroupByRow(List<AggExpr> aggExprs, List<Integer> seenIdxMapping) {
        this(null, null, aggExprs, null);
        this.seenIdxMapping = seenIdxMapping;
    }

    public GroupByRow(GroupByKey key, List<AggState> aggStates,
                      List<AggExpr> aggExprs, List<Set<Object>> seenValuesList)
    {
        this.aggStates = aggStates;
        if (aggStates != null) {
            this.continueCollectingFlags = new boolean[aggStates.size()];
        }
            Arrays.fill(this.continueCollectingFlags, true);
        this.key = key;
        this.aggExprs = aggExprs;
        this.seenValuesList = seenValuesList;
    }

    /**
     * use this ctor only for testing as serialization won't work because the aggExpr and seenValues are missing!
     */
    public GroupByRow(GroupByKey key, List<AggState> aggStates) {
        this(key, aggStates, new ArrayList<AggExpr>(0), new ArrayList<Set<Object>>(0));
    }

    public static GroupByRow createEmptyRow(GroupByKey key, List<AggExpr> aggExprs,
                                            List<Integer> seenValuesIdxMapping,
                                            int seenValuesSize) {
        List<Set<Object>> seenValuesList = new ArrayList<>(seenValuesSize);
        for (int i = 0; i < seenValuesSize; i++) {
            seenValuesList.add(new HashSet<>());
        }

        List<AggState> aggStates = new ArrayList<>(aggExprs.size());
        AggState aggState;

        if (seenValuesSize > 0) {
            int idx = 0;
            for (AggExpr aggExpr : aggExprs) {
                aggState = aggExpr.createAggState();
                if (aggExpr.isDistinct) {
                    aggState.setSeenValuesRef(seenValuesList.get(seenValuesIdxMapping.get(idx)));
                    idx++;
                }
                aggStates.add(aggState);
            }
        } else {
            for (AggExpr aggExpr : aggExprs) {
                aggStates.add(aggExpr.createAggState());
            }
        }

        return new GroupByRow(key, aggStates, aggExprs, seenValuesList);
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

    public static GroupByRow readGroupByRow(List<AggExpr> aggExprs,
                                            List<Integer> seenIdxMapping,
                                            StreamInput in) throws IOException
    {
        GroupByRow row = new GroupByRow(aggExprs, seenIdxMapping);
        row.readFrom(in);
        return row;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        key = GroupByKey.readFromStreamInput(in);

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


        aggStates = new ArrayList<>(aggExprs.size());
        AggExpr aggExpr;
        int seenIdxIndex = 0;
        for (int i = 0; i < aggExprs.size(); i++) {
            aggExpr = aggExprs.get(i);
            aggStates.add(i, aggExpr.createAggState());
            aggStates.get(i).readFrom(in);

            if (aggExpr.isDistinct) {
                aggStates.get(i).setSeenValuesRef(seenValuesList.get(seenIdxMapping.get(seenIdxIndex++)));
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        key.writeTo(out);

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
