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

package io.crate.executor.transport;

import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.ReplacingSymbolVisitor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Replaces all symbols in the querySpec that match the given selectSymbol once the futureCallback
 * triggers with a Literal containing the value from the callback.
 */
class SubSelectSymbolReplacer implements FutureCallback<Object> {

    private final QuerySpec querySpec;
    private final SelectSymbol selectSymbolToReplace;

    SubSelectSymbolReplacer(QuerySpec querySpec, SelectSymbol selectSymbolToReplace) {
        this.querySpec = querySpec;
        this.selectSymbolToReplace = selectSymbolToReplace;
    }

    @Override
    public void onSuccess(@Nullable Object result) {
        querySpec.replace(new Visitor(selectSymbolToReplace, result));
    }

    @Override
    public void onFailure(@Nonnull Throwable t) {
    }

    private static class Visitor extends ReplacingSymbolVisitor<Void> implements Function<Symbol, Symbol> {

        private final SelectSymbol selectSymbolToReplace;
        private final Object value;

        public Visitor(SelectSymbol selectSymbolToReplace, Object value) {
            super(ReplaceMode.MUTATE);
            this.selectSymbolToReplace = selectSymbolToReplace;
            this.value = value;
        }

        @Override
        public Symbol visitSelectSymbol(SelectSymbol selectSymbol, Void context) {
            if (selectSymbol == selectSymbolToReplace) {
                return Literal.of(selectSymbolToReplace.valueType().types().get(0), value);
            }
            return selectSymbol;
        }

        @Nullable
        @Override
        public Symbol apply(@Nullable Symbol input) {
            return process(input, null);
        }
    }
}
