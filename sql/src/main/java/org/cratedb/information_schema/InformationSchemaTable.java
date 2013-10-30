package org.cratedb.information_schema;

import com.google.common.collect.ImmutableMap;

public interface InformationSchemaTable {
    public Iterable<String> cols();
    public ImmutableMap<String, InformationSchemaColumn> fieldMapper();
}
