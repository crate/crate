package org.cratedb;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.UUID;

public class ClusterId implements Streamable {

    private UUID uuid;

    public ClusterId() {
        this.uuid = UUID.randomUUID();
    }

    public ClusterId(UUID uuid) {
        this.uuid = uuid;
    }

    public ClusterId(String uuid) {
        this.uuid = UUID.fromString(uuid);
    }

    public UUID value() {
        return uuid;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        long msb = in.readVLong();
        long lsb = in.readVLong();
        uuid = new UUID(msb, lsb);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(uuid.getMostSignificantBits());
        out.writeVLong(uuid.getLeastSignificantBits());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterId that = (ClusterId) o;

        return uuid.equals(that.uuid);
    }

    @Override
    public int hashCode() {
        return uuid != null ? uuid.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "Cluster [" + uuid.toString() + "]";
    }
}
