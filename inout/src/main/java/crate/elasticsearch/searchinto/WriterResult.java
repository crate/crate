package crate.elasticsearch.searchinto;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class WriterResult implements ToXContent, Streamable {

    private long totalWrites;
    private long failedWrites;
    private long succeededWrites;

    public void setTotalWrites(long totalWrites) {
        this.totalWrites = totalWrites;
    }

    public void setFailedWrites(long failedWrites) {
        this.failedWrites = failedWrites;
    }

    public void setSucceededWrites(long succeededWrites) {
        this.succeededWrites = succeededWrites;
    }

    public long getTotalWrites() {
        return totalWrites;
    }

    public long getFailedWrites() {
        return failedWrites;
    }

    public long getSucceededWrites() {
        return succeededWrites;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        totalWrites = in.readVLong();
        succeededWrites = in.readVLong();
        failedWrites = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalWrites);
        out.writeVLong(succeededWrites);
        out.writeVLong(failedWrites);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder,
            Params params) throws IOException {
        builder.field("total", totalWrites);
        builder.field("succeeded", succeededWrites);
        builder.field("failed", failedWrites);
        return builder;
    }

    public static WriterResult readNew(StreamInput in) throws IOException {
        WriterResult r = new WriterResult();
        r.readFrom(in);
        return r;
    }
}
