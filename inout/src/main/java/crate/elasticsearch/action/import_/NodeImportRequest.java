package crate.elasticsearch.action.import_;

import java.io.IOException;

import org.elasticsearch.action.support.nodes.NodeOperationRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class NodeImportRequest  extends NodeOperationRequest {

    public static final int DEFAULT_BULK_SIZE = 10000;

    private BytesReference source;
    private String index;
    private String type;

    NodeImportRequest() {
    }

    public NodeImportRequest(String nodeId, ImportRequest request) {
        super(request, nodeId);
        this.source = request.source();
        this.index = request.index();
        this.type = request.type();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        source = in.readBytesReference();
        index = in.readOptionalString();
        type = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(source);
        out.writeOptionalString(index);
        out.writeOptionalString(type);
    }

    public BytesReference source() {
        return source;
    }

    public String index() {
        return index;
    }

    public String type() {
        return type;
    }

    public int bulkSize() {
        return DEFAULT_BULK_SIZE;
    }
}
