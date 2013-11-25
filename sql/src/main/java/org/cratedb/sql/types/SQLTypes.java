package org.cratedb.sql.types;

import com.google.common.collect.ImmutableMap;

public class SQLTypes {
    public static final ImmutableMap<String, Class<? extends SQLType>> NUMERIC_TYPES = new ImmutableMap.Builder<String, Class<? extends SQLType>>()
            .put(ByteSQLType.NAME, ByteSQLType.class)
            .put(ShortSQLType.NAME, ShortSQLType.class)
            .put(IntegerSQLType.NAME, IntegerSQLType.class)
            .put(LongSQLType.NAME, LongSQLType.class)
            .put(FloatSQLType.NAME, FloatSQLType.class)
            .put(DoubleSQLType.NAME, DoubleSQLType.class)
            .build();

    public static final ImmutableMap<String, Class<? extends SQLType>> ALL_TYPES = new ImmutableMap.Builder<String, Class<? extends SQLType>>()
            .put(BooleanSQLType.NAME, BooleanSQLType.class)
            .putAll(NUMERIC_TYPES)
            .put(StringSQLType.NAME, StringSQLType.class)
            .put(TimeStampSQLType.NAME, TimeStampSQLType.class)
            .put(CratySQLType.NAME, CratySQLType.class)
            .put(IpSQLType.NAME, IpSQLType.class)
            .build();
}
