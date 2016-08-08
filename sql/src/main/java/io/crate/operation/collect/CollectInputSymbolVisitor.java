/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.collect;

import io.crate.analyze.OrderBy;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.operation.AbstractImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.operation.reference.ReferenceResolver;
import io.crate.operation.reference.doc.lucene.OrderByCollectorExpression;
import io.crate.planner.node.dql.RoutedCollectPhase;
import org.elasticsearch.common.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * convert Symbols into Inputs for evaluation and CollectExpressions
 * that might be treated in a special way
 */
public class CollectInputSymbolVisitor<E extends Input<?>>
        extends AbstractImplementationSymbolVisitor<CollectInputSymbolVisitor.Context> {

    private final ReferenceResolver<E> referenceResolver;

    public static class Context<E> extends AbstractImplementationSymbolVisitor.Context {

        protected ArrayList<E> docLevelExpressions = new ArrayList<>();

        private @Nullable OrderBy orderBy = null;

        public List<E> docLevelExpressions() {
            return docLevelExpressions;
        }

        public void orderBy(@Nullable OrderBy orderBy) {
            this.orderBy = orderBy;
        }

        public @Nullable OrderBy orderBy() {
            return this.orderBy;
        }
    }

    public CollectInputSymbolVisitor(Functions functions, ReferenceResolver<E> referenceResolver) {
        super(functions);
        this.referenceResolver = referenceResolver;
    }

    public Context extractImplementations(RoutedCollectPhase node) {
        Context context = newContext();
        context.orderBy(node.orderBy());
        if (node.toCollect() != null) {
            for (Symbol symbol : node.toCollect()) {
                context.add(process(symbol, context));
            }
        }
        return context;
    }

    @Override
    public Context newContext() {
        return new Context();
    }

    @Override
    public Input<?> visitReference(Reference symbol, Context context) {
        // only doc level references are allowed here, since other granularities
        // should have been resolved by other visitors already
        if (context.orderBy() != null && context.orderBy().orderBySymbols().contains(symbol)) {
            OrderByCollectorExpression docLevelExpression = new OrderByCollectorExpression(symbol, context.orderBy());
            context.docLevelExpressions.add(docLevelExpression);
            return docLevelExpression;
        } else {
            E docLevelExpression = referenceResolver.getImplementation(symbol);
            if (docLevelExpression == null) {
                throw new UnhandledServerException(SymbolFormatter.format("Cannot handle Reference %s", symbol));
            }
            context.docLevelExpressions.add(docLevelExpression);
            return docLevelExpression;
        }
    }
}
