package org.cratedb.action.groupby;

import com.google.common.base.Joiner;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.aggregate.AggState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
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
    public List<AggState> aggStates;


    public List<AggExpr> aggExprs;

    GroupByRow(List<AggExpr> aggExprs) {
        this.aggExprs = aggExprs;
    }

    public GroupByRow(GroupByKey key, List<AggState> aggStates) {
        this.aggStates = aggStates;
        this.key = key;
    }

    public static GroupByRow createEmptyRow(GroupByKey key, List<AggExpr> aggExprs) {
        List<AggState> aggStates = new ArrayList<>(aggExprs.size());
        for (AggExpr aggExpr : aggExprs) {
            aggStates.add(aggExpr.createAggState());
        }

        GroupByRow row = new GroupByRow(key, aggStates);
        row.aggExprs = aggExprs;
        return row;
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
        for (int i = 0; i < aggStates.size(); i++) {
            aggStates.get(i).reduce(otherRow.aggStates.get(i));
        }
    }

    public static GroupByRow readGroupByRow(List<AggExpr> aggExprs, StreamInput in) throws IOException {
        GroupByRow row = new GroupByRow(aggExprs);
        row.readFrom(in);
        return row;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        key = GroupByKey.readFromStreamInput(in);
        aggStates = new ArrayList<>(aggExprs.size());
        AggExpr aggExpr;
        for (int i = 0; i < aggExprs.size(); i++) {
            aggExpr = aggExprs.get(i);
            aggStates.add(i, aggExpr.createAggState());
            aggStates.get(i).readFrom(in);
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
