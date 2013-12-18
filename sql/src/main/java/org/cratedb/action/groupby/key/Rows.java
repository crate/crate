package org.cratedb.action.groupby.key;

import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.sql.ParsedStatement;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public abstract class Rows<Other extends Rows> {

    public abstract GroupByRow getRow();

    public abstract void writeBucket(StreamOutput out, int idx) throws IOException;

    public abstract void readBucket(StreamInput in, int idx) throws IOException;

    public static Rows fromStream(ParsedStatement stmt,
            CacheRecycler cacheRecycler, StreamInput in) throws IOException {
        // note that this reads only into a single bucket
        Rows rows;
        if (stmt.isGlobalAggregate()){
            rows = new GlobalRows(1, stmt);
        } else {
            rows = new GroupTree(1, stmt, cacheRecycler);
        }
        rows.readBucket(in, 0);
        return rows;
    }

    public abstract void merge(Other other);

    public interface RowVisitor{
        public void visit(GroupByRow row);
    }

    public abstract void walk(RowVisitor visitor);
}
