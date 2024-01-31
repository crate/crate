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

package io.crate.metadata.tablefunctions;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.RowType;

/**
 * <p>
 *     Base class for table functions or set returning functions (SRF).
 *     These are functions which return a set of rows.
 * </p>
 *
 * <p>
 *     A row can have multiple columns. These columns are specified by the inner types of {@link #returnType()}
 * </p>
 * <p>
 *     The semantics of SRF can seem a bit odd:
 * </p>
 *
 *  If the SRF is used in place of relations, e.g. the FROM clause, the inner types of the resultType are promoted to top-level columns
 *
 * <pre>
 *     SELECT * FROM srf(...) as t (x, y);
 *     x  | y
 *     ---+---
 *     ...
 *     ...
 * </pre>
 *
 * If the SRF is used in place of expressions, the SRF will return a single column as row type.
 * Except if there is only a single column *within* the row type, in that case it is promoted:
 *
 * <pre>
 *     SELECT srf(...)
 *     srf
 *     ------------
 *     (val1, val2)          <-- row type
 *
 * vs.
 *
 *     SELECT srf(...)
 *     srf
 *     ---------------
 *     1                    <-- {@link #boundSignature()#returnType()} will be a `bigint`,
 *     2                         but {@link #returnType()} will be a RowType which describes the single column.
 * </pre>
 *
 * This promotion behavior of `info()#returnType()` to the inner type of the rowType is necessary to support expressions like:
 *
 * <pre>
 *     SELECT generate_series(1, 2) + 1; // rowType + longType would result in a cast error.
 * </pre>
 *
 *
 */
public abstract class TableFunctionImplementation<T> extends Scalar<Iterable<Row>, T> {

    protected TableFunctionImplementation(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    /**
     * An ObjectType which describes the result of the table function.
     *
     * This can be the same as {@link #boundSignature()#returnType()},
     * but if there is only a single inner type, then {@link #boundSignature()#returnType()} will return that inner-type directly.
     *
     * See the class documentation for more information about that behavior.
     */
    public abstract RowType returnType();

    /**
     * @return true if the records returned by this table function are generated on-demand.
     *         See also {@link BatchIterator#hasLazyResultSet()}
     */
    public abstract boolean hasLazyResultSet();

    @Override
    public Symbol normalizeSymbol(Function function, TransactionContext txnCtx, NodeContext nodeCtx) {
        // Never normalize table functions;
        // The RelationAnalyzer expects a function symbol and can't deal with Literals
        return function;
    }
}
