package io.crate.executor.transport;

import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

public class NodeCollectResponse extends TransportResponse {

    private Object[][] rows;
    private final DataType.Streamer[] streamers;


    public NodeCollectResponse(DataType.Streamer[] streamers) {
        this.streamers = streamers;
    }

    public void rows(Object[][] rows) {
        this.rows = rows;
    }

    public Object[][] rows() {
        return rows;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        rows = new Object[in.readVInt()][];
        for (int r = 0; r < rows.length; r++) {
            rows[r] = new Object[streamers.length];
            for (int c = 0; c < rows[r].length; c++) {
                rows[r][c] = streamers[c].readFrom(in);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(rows.length);
        for (Object[] row : rows) {
            for (int c = 0; c < streamers.length; c++) {
                streamers[c].writeTo(out, row[c]);
            }
        }
    }
}
