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
import io.crate.expression.symbol.Symbol;
import org.elasticsearch.common.Nullable;

import java.util.function.Consumer;
import java.util.function.Function;

public abstract class QueryClause {

    protected Symbol query;
    protected boolean noMatch = false;

    protected QueryClause() {
    }

    protected QueryClause(@Nullable Symbol normalizedQuery) {
        if (normalizedQuery == null) {
            return;
        }
        if (normalizedQuery.symbolType().isValueSymbol()) {
            noMatch = !canMatch(normalizedQuery);
        } else {
            query = normalizedQuery;
        }
    }

    public static boolean canMatch(Symbol query) {
        if (query.symbolType().isValueSymbol()) {
            Object value = ((Input) query).value();
            if (value == null) {
                return false;
            }
            if (value instanceof Boolean) {
                return (Boolean) value;
            } else {
                throw new RuntimeException("Symbol normalized to an invalid value");
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

    public boolean noMatch() {
        return noMatch;
    }

    public void replace(Function<? super Symbol, ? extends Symbol> replaceFunction) {
        if (hasQuery()) {
            Symbol newQuery = replaceFunction.apply(query);
            if (query != newQuery) {
                if (newQuery instanceof Input) {
                    noMatch = !canMatch(newQuery);
                    query = null;
                } else {
                    query = newQuery;
                }
            }
        }
    }

    public void accept(Consumer<? super Symbol> consumer) {
        if (query != null) {
            consumer.accept(query);
        }
    }
}
