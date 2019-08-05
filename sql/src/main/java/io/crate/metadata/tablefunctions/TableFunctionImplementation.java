/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.tablefunctions;

import io.crate.data.Row;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.table.TableInfo;

/**
 * Interface which needs to be implemented by functions returning whole tables as result.
 */
public abstract class TableFunctionImplementation<T> extends Scalar<Iterable<Row>, T> {

    @Override
    public Symbol normalizeSymbol(Function function, TransactionContext txnCtx) {
        // Never normalize table functions;
        // The RelationAnalyzer expects a function symbol and can't deal with Literals
        return function;
    }

    /**
     * Creates the metadata for the table that is generated upon execution of this function. This is the actual return
     * type of the table function.
     * <p>
     * Note: The result type of the {@link FunctionInfo} that is returned by {@link #info()}
     * is ignored for table functions.
     *
     * @return a table info object representing the actual return type of the function.
     */
    public abstract TableInfo createTableInfo();
}
