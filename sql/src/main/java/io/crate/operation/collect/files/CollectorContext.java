package io.crate.operation.collect.files;

import io.crate.operation.reference.file.LineContext;

public class CollectorContext {

    private final LineContext lineContext;

    public CollectorContext() {
        this.lineContext = new LineContext();
    }

    public LineContext lineContext() {
        return lineContext;
    }
}
