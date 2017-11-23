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

package io.crate.analyze.symbol;

import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.types.CollectionType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * @deprecated Please use {@link io.crate.analyze.SymbolEvaluator} which takes care of
 *             evaluating functions and parameters in {@link Symbol}s.
 */
@Deprecated
public abstract class ValueSymbolVisitor<T> extends SymbolVisitor<Void, T> {

    public final com.google.common.base.Function<Symbol, T> function =
        new com.google.common.base.Function<Symbol, T>() {
            @Nullable
            @Override
            public T apply(Symbol input) {
                return process(input);
            }
        };

    public T process(Symbol symbol) {
        return process(symbol, null);
    }

    @Override
    protected T visitSymbol(Symbol symbol, Void context) {
        throw new UnsupportedOperationException(
            SymbolFormatter.format("Unable to get value from symbol: %s", symbol));
    }


    /**
     * @deprecated Symbols must be evaluated using {@link io.crate.analyze.SymbolEvaluator}
     */
    @Deprecated
    public static final ValueSymbolVisitor<Object> VALUE = new ValueSymbolVisitor<Object>() {
        @Override
        public Object visitLiteral(Literal symbol, Void context) {
            return symbol.value();
        }
    };

    /**
     * @deprecated Symbols must be evaluated using {@link io.crate.analyze.SymbolEvaluator}
     */
    @Deprecated
    public static final ValueSymbolVisitor<BytesRef> BYTES_REF = new ValueSymbolVisitor<BytesRef>() {
        @Override
        public BytesRef visitLiteral(Literal symbol, Void context) {
            return DataTypes.STRING.value(symbol.value());
        }
    };

    /**
     * @deprecated Symbols must be evaluated using {@link io.crate.analyze.SymbolEvaluator}
     */
    @Deprecated
    public static final ValueSymbolVisitor<String> STRING = new ValueSymbolVisitor<String>() {
        @Override
        public String visitLiteral(Literal symbol, Void context) {
            return BytesRefs.toString(symbol.value());
        }
    };

    /**
     * @deprecated Symbols must be evaluated using {@link io.crate.analyze.SymbolEvaluator}
     */
    @Deprecated
    public static final ValueSymbolVisitor<List<String>> STRING_LIST = new ValueSymbolVisitor<List<String>>() {
        @Override
        public List<String> visitLiteral(Literal symbol, Void context) {
            assert symbol.valueType() instanceof CollectionType : "literal must be an array or set to get values as list";

            Object[] objects = (Object[]) symbol.value();
            List<String> strings = new ArrayList<>(objects.length);
            for (Object object : objects) {
                strings.add(BytesRefs.toString(object));
            }
            return strings;
        }
    };

    /**
     * @deprecated Symbols must be evaluated using {@link io.crate.analyze.SymbolEvaluator}
     */
    @Deprecated
    public static final ValueSymbolVisitor<Long> LONG = new ValueSymbolVisitor<Long>() {
        @Override
        public Long visitLiteral(Literal symbol, Void context) {
            return DataTypes.LONG.value(symbol.value());
        }
    };

}
