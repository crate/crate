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

    private RowSerializationContext context;
    private List<Integer> seenIdxMapping;

    GroupByRow(RowSerializationContext context) {
        this.context = context;
        this.seenIdxMapping = context.seenIdxMapping;
        this.continueCollectingFlags =
            new boolean[context.aggregateExpressions != null ? context.aggregateExpressions.size(): 0];
        Arrays.fill(this.continueCollectingFlags, true);
    }

    public GroupByRow(GroupByKey key, List<AggState> aggStates, RowSerializationContext context,
                      List<Set<Object>> seenValuesList)
    {
        this.key = key;
        this.aggStates = aggStates;
        this.context = context;
        if (context != null) {
            this.seenIdxMapping = context.seenIdxMapping;
        }
        this.seenValuesList = seenValuesList;

        this.continueCollectingFlags = new boolean[aggStates != null ? aggStates.size() : 0];
        Arrays.fill(this.continueCollectingFlags, true);
    }

    /**
     * use this ctor only for testing as serialization won't work because the aggExpr and seenValues are missing!
     */
    public GroupByRow(GroupByKey key, List<AggState> aggStates) {
        this(key, aggStates, null, new ArrayList<Set<Object>>(0));
    }

    public static GroupByRow createEmptyRow(GroupByKey key,
                                            RowSerializationContext context) {
        List<Set<Object>> seenValuesList = new ArrayList<>();
        for (ParameterInfo _ : context.distinctColumns) {
            seenValuesList.add(new HashSet<>());
        }

        List<AggState> aggStates = new ArrayList<>(context.aggregateExpressions.size());
        AggState aggState;

        if (context.distinctColumns.size() > 0) {
            int idx = 0;
            for (AggExpr aggExpr : context.aggregateExpressions) {
                aggState = aggExpr.createAggState();
                if (aggExpr.isDistinct) {
                    aggState.setSeenValuesRef(seenValuesList.get(context.seenIdxMapping.get(idx)));
                    idx++;
                }
                aggStates.add(aggState);
            }
        } else {
            for (AggExpr aggExpr : context.aggregateExpressions) {
                aggStates.add(aggExpr.createAggState());
            }
        }

        return new GroupByRow(key, aggStates, context, seenValuesList);
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

    public static GroupByRow readGroupByRow(RowSerializationContext context,
                                            StreamInput in) throws IOException
    {
        GroupByRow row = new GroupByRow(context);
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
                values.add(context.typedSeenSerializers[i].readFrom(in));
            }
            seenValuesList.add(values);
        }

        aggStates = new ArrayList<>(context.aggregateExpressions.size());
        AggExpr aggExpr;
        int seenIdxIndex = 0;
        for (int i = 0; i < context.aggregateExpressions.size(); i++) {
            aggExpr = context.aggregateExpressions.get(i);
            aggStates.add(i, aggExpr.createAggState());
            aggStates.get(i).readFrom(in);

            if (aggExpr.isDistinct) {
                aggStates.get(i).setSeenValuesRef(seenValuesList.get(seenIdxMapping.get(seenIdxIndex++)));
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeTo(StreamOutput out) throws IOException {
        key.writeTo(out);

        out.writeVInt(seenValuesList.size());
        int idx = -1;
        for (Set<Object> seenValue : seenValuesList) {
            idx++;
            out.writeVInt(seenValue.size());
            for (Object o : seenValue) {
                context.typedSeenSerializers[idx].writeTo(out, o);
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
