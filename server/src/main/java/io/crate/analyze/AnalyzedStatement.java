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

import java.util.List;
import java.util.function.Consumer;

import org.jetbrains.annotations.Nullable;

import io.crate.expression.symbol.Symbol;

public interface AnalyzedStatement {

    <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context);

    /**
     * Defines whether an operation changes the cluster in any way (write).
     * <p>
     * <p>
     * NOTE: Read only statements with possible writing child statements (like e.g. select using update subquery)
     * must take care about child's write behaviour while returning here.
     * </p>
     */
    boolean isWriteOperation();

    /**
     * Calls the consumer for all top-level symbols within the statement.
     * Implementations must not traverse into sub-relations.
     *
     * Use {@link Relations#traverseDeepSymbols(AnalyzedStatement, Consumer)}
     * For a variant that traverses into sub-relations.
     */
    void visitSymbols(Consumer<? super Symbol> consumer);

    /**
     * Expressions which define the output of the statement.
     * This is only present if the Statement has a ResultSet.
     * For DDL / DML operations without ResultSet this is absent.
     */
    @Nullable
    default List<Symbol> outputs() {
        return null;
    }
}
