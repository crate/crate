package org.cratedb.action;

import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.key.Rows;
import org.cratedb.action.sql.ParsedStatement;
import org.elasticsearch.cache.recycler.CacheRecycler;
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
public class SQLGroupByResult {

    private int reducerIdx;
    private Rows rows;

    /**
     * optimization: the preSerializationResult is set on the mapper
     * after the SQLGroupByResult is sent from the Mapper to the Reducer
     * the result is filled
     *
     * the serialization is basically abused to convert the Collection into a List
     */
    private List<GroupByRow> result;

    public SQLGroupByResult(int reducerIdx, Rows rows) {
        this.rows = rows;
        this.reducerIdx = reducerIdx;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(reducerIdx);
        rows.writeBucket(out, reducerIdx);
    }

    public static SQLGroupByResult readSQLGroupByResult(ParsedStatement stmt,
            CacheRecycler cacheRecycler,
            StreamInput in)
        throws IOException
    {
        int reducerIdx = in.readInt();
        Rows rows = Rows.fromStream(stmt, cacheRecycler, in);
        SQLGroupByResult result = new SQLGroupByResult(reducerIdx, rows);
        return result;
    }

    public Rows rows() {
        return rows;
    }
}
