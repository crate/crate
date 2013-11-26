package org.cratedb;

import com.google.common.collect.ImmutableSet;

public enum DataType {
    BYTE("byte"),
    SHORT("short"),
    INTEGER("integer"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double"),
    BOOLEAN("boolean"),
    STRING("string"),
    TIMESTAMP("timestamp"),
    CRATY("craty"),
    IP("ip");

    private String name;

    private DataType(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        return name;
    }

    public static final ImmutableSet<DataType> NUMERIC_TYPES = ImmutableSet.of(
            BYTE,
            SHORT,
            INTEGER,
            LONG,
            FLOAT,
            DOUBLE
    );

    public static final ImmutableSet<DataType> ALL_TYPES = ImmutableSet.of(
            BYTE,
            SHORT,
            INTEGER,
            LONG,
            FLOAT,
            DOUBLE,
            BOOLEAN,
            STRING,
            TIMESTAMP,
            CRATY,
            IP
    );

    public static final ImmutableSet<DataType> INTEGER_TYPES = ImmutableSet.of(
            BYTE,
            SHORT,
            INTEGER,
            LONG
    );

    public static final ImmutableSet<DataType> DECIMAL_TYPES = ImmutableSet.of(
            FLOAT,
            DOUBLE
    );
}
