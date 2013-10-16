package org.cratedb.action.sql;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class OrderByColumnIdx implements Streamable {

    public Integer index;
    public boolean isAsc;

    public OrderByColumnIdx() {
        // empty ctor for streaming
    }

    public OrderByColumnIdx(int index, boolean isAsc) {
        this.index = index;
        this.isAsc = isAsc;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        index = in.readVInt();
        isAsc = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(index);
        out.writeBoolean(isAsc);
    }

    public static OrderByColumnIdx readFromStream(StreamInput in) throws IOException {
        OrderByColumnIdx result = new OrderByColumnIdx();
        result.readFrom(in);
        return result;
    }
}
