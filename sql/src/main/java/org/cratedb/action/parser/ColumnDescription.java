package org.cratedb.action.parser;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class ColumnDescription implements Streamable {

    public static class Types {
        public final static byte AGGREGATE_COLUMN = 0;
        public final static byte CONSTANT_COLUMN = 1;
    }

    public byte type;

    public ColumnDescription(byte type) {
        this.type = type;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        type = in.readByte();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(type);
    }
}
