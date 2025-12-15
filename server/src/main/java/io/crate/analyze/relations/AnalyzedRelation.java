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

package io.crate.analyze.relations;

import java.util.List;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;

import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.exceptions.AmbiguousColumnException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.Operation;

/**
 * Represents a relation
 *
 * <pre>
 *     {@code
 *      tbl
 *      tableFunction()
 *      <rel> UNION ALL <rel>
 *      SELECT * FROM <rel>
 *      SELECT * FROM <rel>, <rel>
 *      SELECT * FROM <rel> AS t1, <rel> AS t2
 *      SELECT * FROM (SELECT * FROM tbl) as t
 *     }
 * </pre>
 */
public interface AnalyzedRelation extends AnalyzedStatement {

    <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context);

    /**
     * Get a column (as Symbol) by column name. (Columns in this context can be virtual; E.g. computed -> `SELECT x FROM (SELECT 1 + 1 AS x) tbl`;
     * <p>
     *  The contract is that any symbol that is returned by this method must also appear within `outputs`.
     * </p>
     *
     * <p>
     *  An exception to this contract are `AbstractTableRelation` instances:
     *  <ul>
     *  <li>They can support implicit column creation. In that case a `DynamicReference` is returned.</li>
     *  <li>They can return `Reference` symbols for subscripts; In that case on the `topLevel` part of the column appears in the `output`</li>
     *  </ul>
     * </p>
     */
    @Nullable
    Symbol getField(ColumnIdent column, Operation operation, boolean errorOnUnknownObjectKey) throws AmbiguousColumnException, ColumnUnknownException, UnsupportedOperationException;

    /**
     * Like {@link #getField(ColumnIdent, Operation, boolean)}
     * This version sets `errorOnUnknownObjectKey` to true and is for cases where the `ColumnIdent` is derived from a `Symbol` that was previously already analyzed and retrieved via `getField`.
     **/
    @Nullable
    default Symbol getField(ColumnIdent column, Operation operation) throws AmbiguousColumnException, ColumnUnknownException, UnsupportedOperationException {
        return getField(column, operation, true);
    }

    RelationName relationName();

    /**
     * Either the outputs that would be included by `SELECT *` or the list of
     * symbols the user explicitly selected
     */
    @Override
    List<Symbol> outputs();


    /**
     * Returns potentially hidden columns or non-selectable symbols like index references.
     */
    default List<Symbol> hiddenOutputs() {
        return List.of();
    }

    /**
     * Calls the consumer for each top-level symbol in the relation
     * (Arguments/children of function symbols are not visited)
     */
    @Override
    default void visitSymbols(Consumer<? super Symbol> consumer) {
        for (Symbol output : outputs()) {
            consumer.accept(output);
        }
    }

    @Override
    default <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitSelectStatement(this, context);
    }

    @Override
    default boolean isWriteOperation() {
        return false;
    }
}
