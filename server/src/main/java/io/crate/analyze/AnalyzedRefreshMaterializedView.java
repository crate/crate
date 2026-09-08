/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package io.crate.analyze;

import java.util.function.Consumer;

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.doc.DocTableInfo;

public record AnalyzedRefreshMaterializedView(
    DocTableInfo target,
    AnalyzedCreateTableAs replacement
) implements AnalyzedStatement {

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitRefreshMaterializedView(this, context);
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
    }

    @Override
    public boolean isWriteOperation() {
        return true;
    }
}
