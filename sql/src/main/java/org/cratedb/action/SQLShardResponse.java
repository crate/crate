package org.cratedb.action;

import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.cratedb.action.sql.ParsedStatement;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SQLShardResponse extends ActionResponse {

    private ParsedStatement stmt;
    public List<List<Object>> results;

    public SQLShardResponse() {}

    public SQLShardResponse(ParsedStatement stmt) {
        this.stmt = stmt;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int resultLength = in.readVInt();
        results = new ArrayList<>(resultLength);
        if (resultLength > 0) {
            int rowLength = in.readVInt();
            for (int i = 0; i < resultLength; i++) {
                List<Object> row = new ArrayList<>();
                for (int j = 0; j < rowLength; j++) {
                    row.add(stmt.resultColumnList().get(j).returnType().streamer().readFrom(in));
                }
                results.add(row);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(results == null ? 0 : results.size());
        if (results != null && results.size() > 0) {
            out.writeVInt(results.get(0).size());
            for(List<Object> row : results) {

                int idx = -1;
                for (Object value : row) {
                    idx++;
                    stmt.resultColumnList().get(idx).returnType().streamer().writeTo(out, value);
                }
            }
        }
    }

}
