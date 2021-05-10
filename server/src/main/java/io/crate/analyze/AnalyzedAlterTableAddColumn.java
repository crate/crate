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

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;

import java.util.function.Consumer;

public class AnalyzedAlterTableAddColumn implements DDLStatement {

    private final DocTableInfo tableInfo;
    private final AnalyzedTableElements<Symbol> analyzedTableElements;
    private final AnalyzedTableElements<Symbol> analyzedTableElementsWithExpressions;

    public AnalyzedAlterTableAddColumn(DocTableInfo tableInfo,
                                       AnalyzedTableElements<Symbol> analyzedTableElements,
                                       AnalyzedTableElements<Symbol> analyzedTableElementsWithExpressions) {
        this.tableInfo = tableInfo;
        this.analyzedTableElements = analyzedTableElements;
        this.analyzedTableElementsWithExpressions = analyzedTableElementsWithExpressions;
    }

    public DocTableInfo tableInfo() {
        return tableInfo;
    }

    /**
     * List of table elements where every generated or default expression is null-ed out, so all symbols can be safely
     * evaluated to values. See {@link #analyzedTableElementsWithExpressions()} to get table elements including
     * expressions.
     */
    public AnalyzedTableElements<Symbol> analyzedTableElements() {
        return analyzedTableElements;
    }

    /**
     * List of table elements where every possible generated and default expression is evaluated to symbols.
     * These elements (or in concrete the expressions inside) MUST NOT be evaluated, only bound with parameters or
     * subquery result. Evaluating them would destroy the Symbol tree we want to serialize and store inside the table's
     * metadata in order to evaluate it on every write (generated expression) or read (default expression).
     *
     * Putting all together is done by the
     * {@link AnalyzedTableElements#finalizeAndValidate(RelationName, AnalyzedTableElements, AnalyzedTableElements)}
     * method which adds the serialized (printed) symbol tree of these expressions to the evaluated table elements.
     */
    public AnalyzedTableElements<Symbol> analyzedTableElementsWithExpressions() {
        return analyzedTableElementsWithExpressions;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitAlterTableAddColumn(this, context);
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        for (var col : analyzedTableElements.columns()) {
            col.visitSymbols(consumer);
        }
        for (var col : analyzedTableElementsWithExpressions.columns()) {
            col.visitSymbols(consumer);
        }
    }
}
