package org.cratedb.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SQLShardResponse extends ActionResponse {

    public List<List<Object>> results;

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int resultLength = in.readVInt();
        results = new ArrayList<>(resultLength);
        if (resultLength > 0) {
            int rowLength = in.readVInt();
            for (int i = 0; i < resultLength; i++) {
                List row = new ArrayList();
                for (int j = 0; j < rowLength; j++) {
                    row.add(in.readGenericValue());
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
                for (Object value : row) {
                    out.writeGenericValue(value);
                }
            }
        }
    }

}
