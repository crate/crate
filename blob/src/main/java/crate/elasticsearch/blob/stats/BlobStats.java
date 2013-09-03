package crate.elasticsearch.blob.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class BlobStats implements Streamable, ToXContent {

    private long count;
    private long totalUsage;
    private long availableSpace;
    private String location;

    public String location() {
        return location;
    }

    public void location(String location) {
        this.location = location;
    }

    public long count() {
        return count;
    }

    public void count(long count) {
        this.count = count;
    }

    public long availableSpace() {
        return availableSpace;
    }

    public void availableSpace(long availableSpace) {
        this.availableSpace = availableSpace;
    }

    public long totalUsage() {
        return totalUsage;
    }

    public void totalUsage(long totalUsage) {
        this.totalUsage = totalUsage;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        count = in.readVLong();
        totalUsage = in.readVLong();
        availableSpace = in.readVLong();
        location = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        out.writeVLong(totalUsage);
        out.writeVLong(availableSpace);
        out.writeString(location);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject()
            .field("count", count)
            .field("size", totalUsage)
            .field("available_space", availableSpace)
            .field("location", location)
        .endObject();

        return builder;
    }
}
