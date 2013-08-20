package crate.elasticsearch.action.import_;

import org.elasticsearch.action.support.nodes.NodesOperationRequest;
import org.elasticsearch.common.Required;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class ImportRequest extends NodesOperationRequest<ImportRequest> {

    private BytesReference source;

    private String type;
    private String index;

    /**
     * Constructs a new import request against the provided nodes. No nodes provided
     * means it will run against all nodes.
     */
    public ImportRequest(String... nodes) {
        super(nodes);
    }

    /**
     * The query source to execute.
     * @return
     */
    public BytesReference source() {
        return source;
    }

    @Required
    public ImportRequest source(String source) {
        return this.source(new BytesArray(source), false);
    }

    @Required
    public ImportRequest source(BytesReference source, boolean unsafe) {
        this.source = source;
        return this;
    }

    public String type() {
        return this.type;
    }

    public void type(String type) {
        this.type = type;
    }

    public String index() {
        return this.index ;
    }

    public void index(String index) {
        this.index = index;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readOptionalString();
        type = in.readOptionalString();
        source = in.readBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(index);
        out.writeOptionalString(type);
        out.writeBytesReference(source);
    }

}
