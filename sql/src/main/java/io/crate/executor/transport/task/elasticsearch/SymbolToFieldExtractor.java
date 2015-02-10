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

import com.google.common.collect.Lists;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.planner.symbol.*;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for converting a {@link io.crate.planner.symbol.Symbol} to a
 * {@link io.crate.executor.transport.task.elasticsearch.FieldExtractor}.
 *
 * @param <T> The response class a concrete FieldExtractorFactory is operating on
 */
public class SymbolToFieldExtractor<T> {

    private final Visitor<T> visitor;

    public SymbolToFieldExtractor(FieldExtractorFactory extractorFactory) {
        this.visitor = new Visitor<>(extractorFactory);
    }

    public FieldExtractor<T> convert(Symbol symbol, Context context) {
        return visitor.process(symbol, context);
    }

    public abstract static class Context {
        private final List<Reference> references;
        private final Functions functions;
        private String[] referenceNames;

        public Context(Functions functions, int size) {
            this.functions = functions;
            references = new ArrayList<>(size);
        }

        public void addReference(Reference reference) {
            references.add(reference);
        }

        public List<Reference> references() {
            return references;
        }

        public String[] referenceNames() {
            if (referenceNames == null) {
                referenceNames = Lists.transform(references, new com.google.common.base.Function<Reference, String>() {
                    @Nullable
                    @Override
                    public String apply(Reference input) {
                        return input.info().ident().columnIdent().fqn();
                    }
                }).toArray(new String[references.size()]);
            }
            return referenceNames;
        }

        public abstract Object inputValueFor(InputColumn inputColumn);
    }

    static class Visitor<T> extends SymbolVisitor<Context, FieldExtractor<T>> {

        private final FieldExtractorFactory<T, Context> extractorFactory;

        public Visitor(FieldExtractorFactory extractorFactory) {
            this.extractorFactory = extractorFactory;
        }

        @Override
        public FieldExtractor<T> visitReference(Reference reference, Context context) {
            context.addReference(reference);
            return extractorFactory.build(reference, context);
        }

        @Override
        public FieldExtractor<T> visitDynamicReference(DynamicReference symbol, Context context) {
            return visitReference(symbol, context);
        }

        @Override
        public FieldExtractor<T> visitFunction(Function symbol, Context context) {
            List<FieldExtractor<T>> subExtractors = new ArrayList<>(symbol.arguments().size());
            for (Symbol argument : symbol.arguments()) {
                subExtractors.add(process(argument, context));
            }
            return new FunctionExtractor<>((Scalar) context.functions.getSafe(symbol.info().ident()), subExtractors);
        }

        @Override
        public FieldExtractor<T> visitLiteral(Literal symbol, Context context) {
            return new LiteralExtractor<>(symbol.value());
        }

        @Override
        public FieldExtractor<T> visitInputColumn(InputColumn inputColumn, Context context) {
            return new LiteralExtractor<>(context.inputValueFor(inputColumn));
        }

        @Override
        protected FieldExtractor<T> visitSymbol(Symbol symbol, Context context) {
            throw new UnsupportedOperationException(
                    SymbolFormatter.format("Operation not supported with symbol %s", symbol));
        }
    }

}
