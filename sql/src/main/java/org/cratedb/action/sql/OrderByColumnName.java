package org.cratedb.action.sql;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class OrderByColumnName extends OrderByColumnIdx implements Streamable {

    public String name;

    public OrderByColumnName() {
        // empty ctor for streaming
    }

    public OrderByColumnName(String name, int index, boolean isAsc) {
        super(index, isAsc);
        this.name = name;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        name = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
    }
}
