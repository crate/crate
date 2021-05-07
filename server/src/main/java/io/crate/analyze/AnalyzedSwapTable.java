/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.analyze;

import java.util.function.Consumer;

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.doc.DocTableInfo;

public final class AnalyzedSwapTable implements AnalyzedStatement {

    private final DocTableInfo source;
    private final DocTableInfo target;
    private final Symbol dropSource;

    AnalyzedSwapTable(DocTableInfo source, DocTableInfo target, Symbol dropSource) {
        this.source = source;
        this.target = target;
        this.dropSource = dropSource;
    }

    public DocTableInfo source() {
        return source;
    }

    public DocTableInfo target() {
        return target;
    }

    public Symbol dropSource() {
        return dropSource;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitSwapTable(this, context);
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        consumer.accept(dropSource);
    }

    @Override
    public boolean isWriteOperation() {
        return true;
    }

    @Override
    public String toString() {
        return "AnalyzedSwapTable{" +
               "source=" + source +
               ", target=" + target +
               ", dropSource=" + dropSource +
               '}';
    }
}
