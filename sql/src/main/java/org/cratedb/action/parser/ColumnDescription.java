package org.cratedb.action.parser;

import org.cratedb.DataType;

public abstract class ColumnDescription {

    public static class Types {
        public final static byte AGGREGATE_COLUMN = 0;
        public final static byte CONSTANT_COLUMN = 1;
    }

    public byte type;

    public ColumnDescription(byte type) {
        this.type = type;
    }

    public abstract DataType returnType();
}
