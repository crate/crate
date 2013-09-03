package crate.elasticsearch.blob;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class DeleteBlobResponse extends ActionResponse {

    public boolean deleted;

    public DeleteBlobResponse() {
        this.deleted = false;
    }

    public DeleteBlobResponse(boolean deleted) {
        this.deleted = deleted;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        deleted = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(deleted);
    }
}
