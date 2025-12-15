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

package io.crate.lucene;

import java.util.List;

import org.apache.lucene.search.Query;
import org.jetbrains.annotations.Nullable;

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.ScopedRef;

public interface FunctionToQuery {

    /**
     * Returns a Lucene Query with the semantics of the Function.
     *
     * <p>
     * Called if the function is used in the `WHERE` part of a statement when
     * selecting data from a Table with a Lucene index.
     * </p>
     *
     * <p>
     * If `null` is returned, a fallback implementation is used that loads
     * individual records and evaluates the scalar function on each row.
     * </p>
     *
     * <p>
     * Default implementation calls {@link #toQuery(ScopedRef, Literal)},
     * which is there to make it more convenient to implement the functionality.
     * It unwraps the arguments for the common `col <op> literal` case.
     * </p>
     **/
    @Nullable
    default Query toQuery(Function function, LuceneQueryBuilder.Context context) {
        List<Symbol> arguments = function.arguments();
        if (arguments.size() == 2
                && arguments.get(0) instanceof ScopedRef ref
                && arguments.get(1) instanceof Literal<?> literal) {
            return toQuery(ref, literal);
        }
        return null;
    }

    /**
     * See {@link #toQuery(Function, Context)}
     **/
    @Nullable
    default Query toQuery(ScopedRef ref, Literal<?> literal) {
        return null;
    }

    /**
     * Returns a Lucene query with the semantics of the `parent` function.
     *
     * <p>
     * This is similar to {@link #toQuery(Function, Context)} but for cases where *this* function is the `inner` parent and used within another `parent` function.
     * </p>
     *
     * Consider a case like follows:
     *
     * <pre>
     *      WHERE distance(p1, 'POINT (10 20)') = 20
     * </pre>
     *
     * Here `distance` is the inner function and `=` is the parent function.
     * toQuery returns a Query that is equivalent to the full parent function.
     **/
    @Nullable
    default Query toQuery(Function parent, Function inner, Context context) {
        return null;
    }
}
