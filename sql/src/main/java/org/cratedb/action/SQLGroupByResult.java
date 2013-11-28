package org.cratedb.action;

import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.*;


/**
 * Result of a group by operation.
 * Each key represents a row for the SQLResponse.
 *
 * See {@link TransportDistributedSQLAction} for an overview of the workflow how the SQLGroupByResult is used.
 */
public class SQLGroupByResult implements Streamable {

    private List<Integer> seenIdxMapper;
    public List<AggExpr> aggExprs;

    /**
     * optimization: the preSerializationResult is set on the mapper
     * after the SQLGroupByResult is sent from the Mapper to the Reducer
     * the result is filled
     *
     * the serialization is basically abused to convert the Collection into a List
     */
    private List<GroupByRow> result;
    private Collection<GroupByRow> preSerializationResult;

    public SQLGroupByResult(Collection<GroupByRow> result) {
        this.preSerializationResult = result;
    }

    public Collection<GroupByRow> result() {
        if (result != null) {
            return result;
        } else {
            return preSerializationResult;
        }
    }

    SQLGroupByResult(List<AggExpr> aggExprs, List<Integer> seenIdxMapper) {
        this.aggExprs = aggExprs;
        this.seenIdxMapper = seenIdxMapper;
    }

    public int size() {
        if (preSerializationResult != null) {
            return preSerializationResult.size();
        }
        return result.size();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int resultSize = in.readVInt();
        result = new ArrayList<>(resultSize);

        for (int i = 0; i < resultSize; i++) {
            result.add(GroupByRow.readGroupByRow(aggExprs, seenIdxMapper, in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(preSerializationResult.size());
        for (GroupByRow groupByRow : preSerializationResult) {
            groupByRow.writeTo(out);
        }
    }

    public static SQLGroupByResult readSQLGroupByResult(List<AggExpr> aggExprs,
                                                        List<Integer> seenIdxMapper, StreamInput in)
        throws IOException
    {
        SQLGroupByResult result = new SQLGroupByResult(aggExprs, seenIdxMapper);
        result.readFrom(in);
        return result;
    }
}
