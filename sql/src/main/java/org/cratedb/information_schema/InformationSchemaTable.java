package org.cratedb.information_schema;

import com.google.common.collect.ImmutableMap;

public interface InformationSchemaTable {

    public ImmutableMap<String, InformationSchemaColumn> fieldMapper();
    public Iterable<String> cols();
}
