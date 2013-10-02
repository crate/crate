package org.cratedb.sql.facet;

import org.cratedb.action.sql.ParsedStatement;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import java.io.IOException;
import java.util.Map;

public class InternalSQLFacet extends InternalFacet implements SQLFacet {

    private static final BytesReference STREAM_TYPE = new HashedBytesArray(Strings.toUTF8Bytes(
            "sql"));

    private Object facet;
    private String scriptLang;
    private String reduceScript;
    private Map<String, Object> reduceParams;
    private ScriptService scriptService;
    private Client client;
    private ReduceContext reduceContext;
    private Object[][] rows;
    private long rowCount;

    public InternalSQLFacet(String facetName) {
        super(facetName);
    }

    public InternalSQLFacet() {
        super();
    }


    public static void registerStreams() {
        Streams.registerStream(new SQLFacetStream(), STREAM_TYPE);
    }

    @Override
    public BytesReference streamType() {
        return STREAM_TYPE;
    }

    /**
     * This method is called on the collecting node. In this implementation no reduction is done
     * here, since the statement is not known
     * the real reduce happens in {@link SQLFacet#reduce(org.cratedb.action.sql.ParsedStatement)}
     */
    @Override
    public Facet reduce(ReduceContext reduceContext) {
        this.reduceContext = reduceContext;
        return this;
    }

    @Override
    public String getType() {
        return SQLFacet.TYPE;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        rowCount = in.readVLong();
        if (rowCount == 0) return;
        int numRows = in.readInt();
        if (numRows == 0) return;
        int numCols = in.readInt();
        rows = new Object[numRows][numCols];
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numCols; j++) {
                rows[i][j] = in.readGenericValue();
            }
        }

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(rowCount);
        if (rowCount == 0) return;
        int numCols = 0;
        if (rows == null || rows.length == 0) {
            out.writeInt(0);
            return;
        } else {
            out.writeInt(rows.length);
            numCols = rows[0].length;
        }
        for (int i = 0; i < rows.length; i++) {
            for (int j = 0; j < numCols; j++) {
                out.writeGenericValue(rows[i][j]);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // at this point we do not support xcontent output
        throw new UnsupportedOperationException();
    }

    public static InternalSQLFacet readMapReduceFacet(StreamInput in) throws IOException {
        InternalSQLFacet facet = new InternalSQLFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override
    public void reduce(ParsedStatement stmt) {
        // Currently only the rowcount gets accumulated
        for (Facet facet : reduceContext.facets()) {
            if (facet != this) {
                rowCount += ((InternalSQLFacet) facet).rowCount();
            }
        }
    }

    @Override
    public Object[][] rows() {
        return rows;
    }

    @Override
    public long rowCount() {
        return rowCount;
    }

    public void rowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    private static class SQLFacetStream implements InternalFacet.Stream {

        @Override
        public Facet readFacet(StreamInput in) throws IOException {
            // this gets executed on the HandlerNode, where the statement should have already
            // been parsed.
            return InternalSQLFacet.readMapReduceFacet(in);
        }
    }
}
