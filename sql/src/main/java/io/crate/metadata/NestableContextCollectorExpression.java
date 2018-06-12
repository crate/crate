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

package io.crate.metadata;

import io.crate.execution.engine.collect.NestableCollectExpression;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.function.Function;

public abstract class NestableContextCollectorExpression<TRow, TReturnValue> implements NestableCollectExpression<TRow, TReturnValue> {

    protected TRow row;

    @Override
    public void setNextRow(TRow row) {
        this.row = row;
    }

    public static <TRow, TReturnValue> NestableCollectExpression<TRow, TReturnValue> constant(TReturnValue val) {
        return new ConstantNestableContextCollectorExpression<>(val);
    }

    public static <TRow, TReturnValue> NestableCollectExpression<TRow, TReturnValue> forFunction(Function<TRow, TReturnValue> fun) {
        return new FuncExpression<>(fun);
    }

    public static <TRow> NestableCollectExpression<TRow, BytesRef> objToBytesRef(Function<TRow, Object> fun) {
        return forFunction(fun.andThen(BytesRefs::toBytesRef));
    }

    public static <TRow, TIntermediate> NestableCollectExpression<TRow, Object> withNullableProperty(Function<TRow, TIntermediate> getProperty,
                                                                                                     Function<TIntermediate, Object> extractValue) {
        return new NestableContextCollectorExpression<TRow, Object>() {

            @Override
            public Object value() {
                TIntermediate intermediate = getProperty.apply(row);
                if (intermediate == null) {
                    return null;
                }
                return extractValue.apply(intermediate);
            }
        };
    }

    private static class FuncExpression<TRow, TReturnVal> extends NestableContextCollectorExpression<TRow, TReturnVal> {

        private final Function<TRow, TReturnVal> f;

        FuncExpression(Function<TRow, TReturnVal> f) {
            this.f = f;
        }

        @Override
        public TReturnVal value() {
            return f.apply(row);
        }
    }

    private static class ConstantNestableContextCollectorExpression<TRow, TReturnValue> extends NestableContextCollectorExpression<TRow, TReturnValue> {
        private final TReturnValue val;

        ConstantNestableContextCollectorExpression(TReturnValue val) {
            this.val = val;
        }

        @Override
        public TReturnValue value() {
            return val;
        }
    }
}
