package io.crate.metadata.table;

import com.google.common.collect.ImmutableMap;

import java.util.Locale;

public abstract class AbstractTableInfo implements TableInfo {

    @Override
    public String toString() {
        return String.format(Locale.ENGLISH, "%s.%s", ident().schema(), ident().name());
    }

    @Override
    public ImmutableMap<String, Object> tableParameters() {
        return ImmutableMap.of();
    }

}
