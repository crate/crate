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

package io.crate.execution.dml.upsert;

import io.crate.analyze.SymbolEvaluator;
import io.crate.common.collections.Maps;
import io.crate.data.Input;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.InputFactory;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class GeneratedColsFromRawInsertSource implements InsertSourceGen {

    private final Map<Reference, Input<?>> generatedCols;
    private final List<CollectExpression<Map<String, Object>, ?>> expressions;
    private final Map<Reference, Object> defaults;

    GeneratedColsFromRawInsertSource(TransactionContext txnCtx,
                                     NodeContext nodeCtx,
                                     List<GeneratedReference> generatedColumns,
                                     List<Reference> defaultExpressionColumns) {
        InputFactory inputFactory = new InputFactory(nodeCtx);
        InputFactory.Context<CollectExpression<Map<String, Object>, ?>> ctx =
            inputFactory.ctxForRefs(txnCtx, FromSourceRefResolver.WITHOUT_PARTITIONED_BY_REFS);
        this.generatedCols = new HashMap<>(generatedColumns.size());
        generatedColumns.forEach(r -> generatedCols.put(r, ctx.add(r.generatedExpression())));
        expressions = ctx.expressions();
        defaults = buildDefaults(defaultExpressionColumns, txnCtx, nodeCtx);
    }

    @Override
    public Map<String, Object> generateSourceAndCheckConstraints(Object[] values) {
        String rawSource = (String) values[0];
        Map<String, Object> source = XContentHelper.toMap(new BytesArray(rawSource), XContentType.JSON);
        mixinDefaults(source, defaults);
        for (int i = 0; i < expressions.size(); i++) {
            expressions.get(i).setNextRow(source);
        }
        for (Map.Entry<Reference, Input<?>> entry : generatedCols.entrySet()) {
            var reference = entry.getKey();
            var value = entry.getValue().value();
            var valueForInsert = reference
                .valueType()
                .valueForInsert(value);
            source.putIfAbsent(reference.column().fqn(), valueForInsert);
        }
        return source;
    }

    private Map<Reference, Object> buildDefaults(List<Reference> defaults,
                                                 TransactionContext txnCtx,
                                                 NodeContext nodeCtx) {
        HashMap<Reference, Object> m = new HashMap<>();
        for (Reference ref : defaults) {
            Object val = SymbolEvaluator.evaluateWithoutParams(txnCtx, nodeCtx, ref.defaultExpression());
            m.put(ref, val);
        }
        return m;
    }

    private static void mixinDefaults(Map<String, Object> source, Map<Reference, Object> defaults) {
        for (var entry : defaults.entrySet()) {
            ColumnIdent column = entry.getKey().column();
            Maps.mergeInto(source, column.name(), column.path(), entry.getValue(), Map::putIfAbsent);
        }
    }
}
