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
import io.crate.lucene.FieldTypeLookup;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.operation.InputFactory;
import io.crate.operation.reference.ReferenceResolver;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.OrderByCollectorExpression;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.types.DataType;
import io.crate.types.IpType;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.function.Function;

/**
 * Specialized InputFactory for Lucene symbols/expressions.
 *
 * See {@link InputFactory} for an explanation what a InputFactory does.
 */
public class DocInputFactory {

    private final FieldTypeLookup fieldTypeLookup;
    private final ReferenceResolver<? extends LuceneCollectorExpression<?>> referenceResolver;
    private final InputFactory inputFactory;

    public DocInputFactory(Functions functions,
                           FieldTypeLookup fieldTypeLookup,
                           ReferenceResolver<? extends LuceneCollectorExpression<?>> referenceResolver) {
        this.inputFactory = new InputFactory(functions);
        this.fieldTypeLookup = fieldTypeLookup;
        this.referenceResolver = referenceResolver;
    }

    public InputFactory.Context<? extends LuceneCollectorExpression<?>> extractImplementations(RoutedCollectPhase phase) {
        OrderBy orderBy = phase.orderBy();
        ReferenceResolver<? extends LuceneCollectorExpression<?>> refResolver;
        if (orderBy == null) {
            refResolver = referenceResolver;
        } else {
            refResolver = ref -> {
                if (orderBy.orderBySymbols().contains(ref)) {
                    return getOrderByExpression(orderBy, ref);
                }
                return referenceResolver.getImplementation(ref);
            };
        }
        InputFactory.Context<? extends LuceneCollectorExpression<?>> ctx = inputFactory.ctxForRefs(refResolver);
        ctx.add(phase.toCollect());
        return ctx;
    }

    private LuceneCollectorExpression<?> getOrderByExpression(OrderBy orderBy, Reference ref) {
        DataType dataType = ref.valueType();
        Function<Object, Object> valueConversion;
        switch (dataType.id()) {
            case IpType.ID:
                MappedFieldType mappedFieldType = fieldTypeLookup.get(ref.column().fqn());
                if (mappedFieldType == null) {
                    valueConversion = dataType::value;
                } else {
                    // need to convert binary bytesRef to BytesRef string
                    valueConversion = i -> dataType.value(mappedFieldType.valueForDisplay(i));
                }
                break;

            default:
                valueConversion = dataType::value;
        }
        return new OrderByCollectorExpression(ref, orderBy, valueConversion);
    }

    public InputFactory.Context<? extends LuceneCollectorExpression<?>> getCtx() {
        return inputFactory.ctxForRefs(referenceResolver);
    }
}
