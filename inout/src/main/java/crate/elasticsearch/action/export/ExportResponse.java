package crate.elasticsearch.action.export;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.rest.action.support.RestActions.buildBroadcastShardsHeader;

/**
 * The response of the count action.
 */
public class ExportResponse extends BroadcastOperationResponse implements ToXContent {


    private List<ShardExportResponse> responses;
    private long totalExported;

    public ExportResponse(List<ShardExportResponse> responses, int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
        //To change body of created methods use File | Settings | File Templates.
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.responses = responses;
        for (ShardExportResponse r : this.responses) {
            totalExported += r.getNumExported();
        }
    }

    public ExportResponse() {

    }


    public long getTotalExported() {
        return totalExported;
    }


    public List<ShardExportResponse> getResponses() {
        return responses;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        totalExported = in.readVLong();
        int numResponses = in.readVInt();
        responses = new ArrayList<ShardExportResponse>(numResponses);
        for (int i = 0; i < numResponses; i++) {
            responses.add(ShardExportResponse.readNew(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(totalExported);
        out.writeVInt(responses.size());
        for (ShardExportResponse response : responses) {
            response.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("exports");
        for (ShardExportResponse r : this.responses) {
            r.toXContent(builder, params);
        }
        builder.endArray();
        builder.field("totalExported", totalExported);
        buildBroadcastShardsHeader(builder, this);
        builder.endObject();
        return builder;
    }
}
