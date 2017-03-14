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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import io.crate.analyze.symbol.*;
import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for converting a {@link io.crate.analyze.symbol.Symbol} to a
 * {@link com.google.common.base.Function}
 *
 * @param <T> The response class a concrete FieldExtractorFactory is operating on
 */
public class SymbolToFieldExtractor<T> {

    private final Visitor<T> visitor;

    public SymbolToFieldExtractor(FieldExtractorFactory extractorFactory) {
        this.visitor = new Visitor<>(extractorFactory);
    }

    public Function<T, Object> convert(Symbol symbol, Context context) {
        return visitor.process(symbol, context);
    }

    public abstract static class Context {
        private final io.crate.metadata.Functions functions;

        public Context(io.crate.metadata.Functions functions, int size) {
            this.functions = functions;
        }

        public abstract Object inputValueFor(InputColumn inputColumn);

        @Nullable
        public Object referenceValue(Reference reference) {
            return null;
        }
    }


    private static <T> Function<T, Object> constant(Object value) {
        //noinspection unchecked
        return (Function<T, Object>) Functions.constant(value);
    }

    static class Visitor<T> extends SymbolVisitor<Context, Function<T, Object>> {

        private final FieldExtractorFactory<T, Context> extractorFactory;

        public Visitor(FieldExtractorFactory extractorFactory) {
            this.extractorFactory = extractorFactory;
        }

        @Override
        public Function<T, Object> visitReference(Reference reference, Context context) {
            Object value = context.referenceValue(reference);
            if (value != null) {
                return constant(value);
            }
            return extractorFactory.build(reference, context);
        }

        @Override
        public Function<T, Object> visitDynamicReference(DynamicReference symbol, Context context) {
            return visitReference(symbol, context);
        }

        @Override
        public Function<T, Object> visitFunction(io.crate.analyze.symbol.Function symbol, Context context) {
            List<Function<T, Object>> subExtractors = new ArrayList<>(symbol.arguments().size());
            for (Symbol argument : symbol.arguments()) {
                subExtractors.add(process(argument, context));
            }
            FunctionIdent ident = symbol.info().ident();
            return new FunctionExtractor<>(
                (Scalar) context.functions.getSafe(ident.schema(), ident.name(), ident.argumentTypes()), subExtractors);
        }

        @Override
        public Function<T, Object> visitLiteral(Literal symbol, Context context) {
            return constant(symbol.value());
        }

        @Override
        public Function<T, Object> visitInputColumn(InputColumn inputColumn, Context context) {
            return constant(context.inputValueFor(inputColumn));
        }

        @Override
        protected Function<T, Object> visitSymbol(Symbol symbol, Context context) {
            throw new UnsupportedOperationException(
                SymbolFormatter.format("Operation not supported with symbol %s", symbol));
        }
    }

}
