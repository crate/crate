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


    private Map<String, AggFunction> aggregateFunctions;
    public List<AggExpr> aggExprs;


    public GroupByRow() {
    }

    public GroupByRow(Map<String, AggFunction> aggregateFunctions, List<AggExpr> aggExprs) {
        this.aggregateFunctions = aggregateFunctions;
        this.aggExprs = aggExprs;
    }

    public GroupByRow(GroupByKey key, List<AggState> aggStates) {
        this.aggStates = aggStates;
        this.key = key;
    }

    public static GroupByRow createEmptyRow(GroupByKey key,
                                            List<AggExpr> aggExprs,
                                            Map<String, AggFunction> aggregateFunctions) {
        List<AggState> aggStates = new ArrayList<>(aggExprs.size());

        AggExpr aggExpr;
        for (int i = 0; i < aggExprs.size(); i++) {
            aggExpr = aggExprs.get(i);
            aggStates.add(aggregateFunctions.get(aggExpr.functionName).createAggState(aggExpr));
        }

        GroupByRow row = new GroupByRow(key, aggStates);
        row.aggregateFunctions = aggregateFunctions;
        row.aggExprs = aggExprs;
        return row;
    }

    /**
     * get key value or state.
     * Use {@link org.cratedb.action.sql.ParsedStatement#idxMap} to access by ResultColumnList index
     */
    public Object get(int idx) {
        if (idx > (key.size() - 1)) {
            return aggStates.get(idx - key.size()).value();
        }
        return key.get(idx);
    }

    /**
     * get the AggExpr for the idx/position in the resultColumnList of this GroupByRow
     *
     * Example::
     *
     *      select count(*), avg(income) from employees group by departement;
     *
     *      getAggExpr(0) -> AggExpr: count(*)
     *      getAggExpr(1) -> AggExpr: avg(income)
     *
     * @param idx the position in the resultcolumnList
     * @return the AggExpr or null if none was found
     */
    public AggExpr getAggExpr(int idx) {
        if (aggExprs == null) { return null; }
        try {
            return aggExprs.get(idx - key.size());
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
    }

    /**
     * get the index/position of an AggExpr in this row
     * @param aggExpr the AggExpr to get the index of
     * @return the index/position as an int, -1 if AggExpr does not exist
     */
    public int getIdx(AggExpr aggExpr) {
        if (aggExprs == null) { return -1; }
        int idx = aggExprs.indexOf(aggExpr);
        return (idx >= 0 ? idx += key.size() : idx);
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
    public synchronized  void merge(GroupByRow otherRow) {
        for (int i = 0; i < aggStates.size(); i++) {
            aggStates.get(i).reduce(otherRow.aggStates.get(i));
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
        aggStates = new ArrayList<>(aggExprs.size());
        AggExpr aggExpr;
        for (int i = 0; i < aggExprs.size(); i++) {
            aggExpr = aggExprs.get(i);
            aggStates.add(i, aggregateFunctions.get(aggExpr.functionName).createAggState(aggExpr));
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
