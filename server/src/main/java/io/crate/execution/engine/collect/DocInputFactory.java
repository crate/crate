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

package io.crate.execution.engine.collect;

import io.crate.analyze.OrderBy;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.OrderByCollectorExpression;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.types.DataType;

/**
 * Specialized InputFactory for Lucene symbols/expressions.
 *
 * See {@link InputFactory} for an explanation what a InputFactory does.
 */
public class DocInputFactory {

    private final ReferenceResolver<? extends LuceneCollectorExpression<?>> referenceResolver;
    private final InputFactory inputFactory;

    public DocInputFactory(NodeContext nodeCtx,
                           ReferenceResolver<? extends LuceneCollectorExpression<?>> referenceResolver) {
        this.inputFactory = new InputFactory(nodeCtx);
        this.referenceResolver = referenceResolver;
    }

    public InputFactory.Context<? extends LuceneCollectorExpression<?>> extractImplementations(TransactionContext txnCtx,
                                                                                               RoutedCollectPhase phase) {
        OrderBy orderBy = phase.orderBy();
        ReferenceResolver<? extends LuceneCollectorExpression<?>> refResolver;
        if (orderBy == null) {
            refResolver = referenceResolver;
        } else {
            refResolver = ref -> {
                if (orderBy.orderBySymbols().contains(ref)) {
                    DataType<?> dataType = ref.valueType();
                    return new OrderByCollectorExpression(ref, orderBy, dataType::sanitizeType);
                }
                return referenceResolver.getImplementation(ref);
            };
        }
        InputFactory.Context<? extends LuceneCollectorExpression<?>> ctx = inputFactory.ctxForRefs(txnCtx, refResolver);
        ctx.add(phase.toCollect());
        return ctx;
    }

    public InputFactory.Context<? extends LuceneCollectorExpression<?>> getCtx(TransactionContext txnCtx) {
        return inputFactory.ctxForRefs(txnCtx, referenceResolver);
    }
}
