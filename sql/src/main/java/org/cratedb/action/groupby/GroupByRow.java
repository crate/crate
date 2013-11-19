package org.cratedb.action.groupby;

import com.google.common.base.Joiner;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.aggregate.AggState;
import org.cratedb.action.groupby.aggregate.AggStateReader;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
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
    public Integer[] idxMap;

    public GroupByRow() {}

    public GroupByRow(Integer[] idxMap, GroupByKey key, AggState... aggStates) {
        this.idxMap = idxMap;
        this.aggStates = aggStates;
        this.key = key;
    }

    public static GroupByRow createEmptyRow(Integer[] idxMap,
                                            GroupByKey key,
                                            List<AggExpr> aggExprs,
                                            Map<String, AggFunction> aggregateFunctions) {
        GroupByRow row = new GroupByRow();
        row.key = key;
        row.aggStates = new AggState[aggExprs.size()];
        row.idxMap = idxMap;

        for (int i = 0; i < row.aggStates.length; i++) {
            row.aggStates[i] = aggregateFunctions.get(aggExprs.get(i).functionName).createAggState();
        }

        return row;
    }

    public Object get(int idx) {
        int realIdx = idxMap[idx];
        if (realIdx > (key.size() - 1)) {
            return aggStates[realIdx - key.size()].value();
        }
        return key.get(realIdx);
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
        aggStates = new AggState[in.readVInt()];

        for (int i = 0; i < aggStates.length; i++) {
            aggStates[i] = AggStateReader.readFrom(in);
        }

        idxMap = new Integer[in.readVInt()];
        for (int i = 0; i < idxMap.length; i++) {
            idxMap[i] = in.readVInt();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        key.writeTo(out);
        out.writeVInt(aggStates.length);
        for (AggState aggState : aggStates) {
            aggState.writeTo(out);
        }

        out.writeVInt(idxMap.length);
        for (Integer idx : idxMap) {
            out.writeVInt(idx);
        }
    }

    public void merge(GroupByRow otherRow) {
        for (int i = 0; i < aggStates.length; i++) {
            aggStates[i].merge(otherRow.aggStates[i]);
        }
    }
}
