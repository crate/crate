package org.cratedb.action.groupby;

import com.google.common.base.Joiner;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.aggregate.AggState;
import org.cratedb.action.groupby.aggregate.AggStateReader;
import org.cratedb.action.parser.ColumnDescription;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

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

    public GroupByKey key;
    public AggState[] aggStates;

    public GroupByRow() {

    }

    public GroupByRow(GroupByKey key, AggState... aggStates) {
        this.aggStates = aggStates;
        this.key = key;
    }

    public static GroupByRow createEmptyRow(GroupByKey key,
                                            List<AggExpr> aggExprs,
                                            Map<String, AggFunction> aggregateFunctions) {
        GroupByRow row = new GroupByRow();
        row.key = key;
        row.aggStates = new AggState[aggExprs.size()];

        for (int i = 0; i < row.aggStates.length; i++) {
            row.aggStates[i] = aggregateFunctions.get(aggExprs.get(i).functionName).createAggState();
        }

        return row;
    }

    public Object get(int idx) {
        return null;
    }

    @Override
    public String toString() {
        return "GroupByRow{" +
            "aggregateStates=" + Joiner.on(", ").join(aggStates) +
            ", key=" + key +
            '}';
    }

    public int size() {
        return key.size() + aggStates.length;
    }

    public static GroupByRow readGroupByRow(StreamInput in) throws IOException {
        GroupByRow row = new GroupByRow();
        row.readFrom(in);
        return row;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        key = GroupByKey.readFromStreamInput(in);
        //int numAggregateStates = in.readVInt();
        //aggregateStates = new HashMap<>(numAggregateStates);
        //for (int i = 0; i < numAggregateStates; i++) {
        //    int key = in.readVInt();
        //    AggState state = AggStateReader.readFrom(in);
        //    aggregateStates.put(key, state);
        //}

        //int numRegularColumns = in.readVInt();
        //regularColumns = new HashMap<>(numRegularColumns);
        //for (int i = 0; i < numRegularColumns; i++) {
        //    int key = in.readVInt();
        //    Object value = in.readGenericValue();
        //    regularColumns.put(key, value);
        //}
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        key.writeTo(out);
        //out.writeVInt(aggregateStates.size());
        //for (Map.Entry<Integer, AggState> entry : aggregateStates.entrySet()) {
        //    out.writeVInt(entry.getKey());
        //    entry.getValue().writeTo(out);
        //}

        //if (regularColumns == null) {
        //    out.writeVInt(0);
        //} else {
        //    out.writeVInt(regularColumns.size());
        //    for (Map.Entry<Integer, Object> entry : regularColumns.entrySet()) {
        //        out.writeVInt(entry.getKey());
        //        out.writeGenericValue(entry.getValue());
        //    }
        //}
    }

    public void merge(GroupByRow otherRow) {
        //for (Map.Entry<Integer, AggState> thisEntry : aggregateStates.entrySet()) {
        //    AggState currentValue = thisEntry.getValue();
        //    AggState otherValue = otherRow.aggregateStates.get(thisEntry.getKey());
        //    currentValue.merge(otherValue);
        //    thisEntry.setValue(currentValue);
        //}
    }
}
