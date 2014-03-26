package io.crate.operation.collect.files;

import io.crate.metadata.ColumnIdent;
import io.crate.operation.reference.file.LineContext;

import java.util.HashSet;
import java.util.Set;

public class CollectorContext {

    private final LineContext lineContext;
    private final Set<ColumnIdent> prefetchColumns = new HashSet<>();

    public CollectorContext() {
        this.lineContext = new LineContext(this);
    }

    public LineContext lineContext() {
        return lineContext;
    }

    public void addPrefetchColumn(ColumnIdent columnIdent) {
        prefetchColumns.add(columnIdent);
    }

    public Set<ColumnIdent> prefetchColumns() {
        return prefetchColumns;
    }
}
