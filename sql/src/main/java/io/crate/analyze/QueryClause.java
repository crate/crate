/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.data.Input;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import javax.annotation.Nullable;

import java.util.function.Consumer;

public abstract class QueryClause {

    protected final Symbol query;

    protected QueryClause(@Nullable Symbol normalizedQuery) {
        this.query = normalizedQuery;
    }

    public static boolean canMatch(Symbol query) {
        if (query instanceof Input) {
            Object value = ((Input<?>) query).value();
            if (value == null) {
                return false;
            }
            if (value instanceof Boolean) {
                return (Boolean) value;
            } else {
                throw new IllegalArgumentException("Expected query value to be of type `Boolean`, but got: " + value);
            }
        }
        return true;
    }

    public boolean hasQuery() {
        return query != null;
    }

    @Nullable
    public Symbol query() {
        return query;
    }

    public Symbol queryOrFallback() {
        return query == null ? Literal.BOOLEAN_TRUE : query;
    }

    public void accept(Consumer<? super Symbol> consumer) {
        if (query != null) {
            consumer.accept(query);
        }
    }
}
