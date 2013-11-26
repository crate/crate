package org.cratedb.action.groupby;

import com.google.common.base.Joiner;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.aggregate.AggState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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


    private Map<String, AggFunction> aggregateFunctions;
    private List<AggExpr> aggExprs;


    public GroupByRow() {
    }

    public GroupByRow(Map<String, AggFunction> aggregateFunctions, List<AggExpr> aggExprs) {
        this.aggregateFunctions = aggregateFunctions;
        this.aggExprs = aggExprs;
    }

    public GroupByRow(GroupByKey key, AggState... aggStates) {
        this.aggStates = aggStates;
        this.key = key;
    }

    public static GroupByRow createEmptyRow(GroupByKey key,
                                            AggFunction[] aggFunctions) {
        GroupByRow row = new GroupByRow();
        row.key = key;
        row.aggStates = new AggState[aggFunctions.length];

        for (int i = 0; i < row.aggStates.length; i++) {
            row.aggStates[i] = aggFunctions[i].createAggState();
        }

        return row;
    }

    /**
     * get key value or state.
     * Use {@link org.cratedb.action.sql.ParsedStatement#idxMap} to access by ResultColumnList index
     */
    public Object get(int idx) {
        if (idx > (key.size() - 1)) {
            return aggStates[idx - key.size()].value();
        }
        return key.get(idx);
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

    public synchronized  void merge(GroupByRow otherRow) {
        for (int i = 0; i < aggStates.length; i++) {
            aggStates[i].merge(otherRow.aggStates[i]);
        }
    }

    public static GroupByRow readGroupByRow(Map<String, AggFunction> aggregateFunctions,
                                            List<AggExpr> aggExprs, StreamInput in) throws IOException {
        GroupByRow row = new GroupByRow(aggregateFunctions, aggExprs);
        row.readFrom(in);
        return row;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        key = GroupByKey.readFromStreamInput(in);
        aggStates = new AggState[aggExprs.size()];
        for (int i = 0; i < aggStates.length; i++) {
            aggStates[i] = aggregateFunctions.get(aggExprs.get(i).functionName).createAggState();
            aggStates[i].readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        key.writeTo(out);
        for (AggState aggState : aggStates) {
            aggState.writeTo(out);
        }
    }

}
