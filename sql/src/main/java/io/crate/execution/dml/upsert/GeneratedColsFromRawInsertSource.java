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

package io.crate.execution.dml.upsert;

import io.crate.data.Input;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.InputFactory;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.Functions;
import io.crate.metadata.GeneratedReference;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class GeneratedColsFromRawInsertSource implements InsertSourceGen {

    private final Map<ColumnIdent, Input<?>> generatedCols;
    private final List<CollectExpression<Map<String, Object>, ?>> expressions;

    GeneratedColsFromRawInsertSource(
        TransactionContext txnCtx,
        Functions functions,
        List<GeneratedReference> generatedColumns,
        List<Reference> defaultExpressionColumns) {

        InputFactory inputFactory = new InputFactory(functions);
        InputFactory.Context<CollectExpression<Map<String, Object>, ?>> ctx =
            inputFactory.ctxForRefs(txnCtx, FromSourceRefResolver.INSTANCE);
        this.generatedCols = new HashMap<>(generatedColumns.size() + defaultExpressionColumns.size());
        defaultExpressionColumns.forEach(r -> generatedCols.put(r.column(), ctx.add(r.defaultExpression())));
        generatedColumns.forEach(r -> generatedCols.put(r.column(), ctx.add(r.generatedExpression())));
        expressions = ctx.expressions();
    }

    @Override
    public void checkConstraints(Object[] values) {
    }

    @Override
    public BytesReference generateSource(Object[] values) throws IOException {
        String rawSource = (String) values[0];
        Map<String, Object> source = XContentHelper.convertToMap(new BytesArray(rawSource), false, XContentType.JSON).v2();
        for (int i = 0; i < expressions.size(); i++) {
            expressions.get(i).setNextRow(source);
        }
        for (Map.Entry<ColumnIdent, Input<?>> entry : generatedCols.entrySet()) {
            source.putIfAbsent(entry.getKey().fqn(), entry.getValue().value());
        }
        return BytesReference.bytes(XContentFactory.jsonBuilder().map(source));
    }

}
