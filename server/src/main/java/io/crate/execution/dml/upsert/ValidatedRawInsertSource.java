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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.ParameterizedXContentParser;
import org.elasticsearch.index.mapper.StrictDynamicMappingException;

import com.fasterxml.jackson.core.JsonParseException;

import io.crate.analyze.SymbolEvaluator;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Maps;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ConversionException;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.InputFactory;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.NodeContext;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;

public class ValidatedRawInsertSource implements InsertSourceGen, ParameterizedXContentParser.FieldParser {

    private final DocTableInfo table;
    private final TransactionContext txnCtx;
    private final NodeContext nodeCtx;
    private final InputFactory inputFactory;
    private final CheckConstraints<Map<String, Object>, CollectExpression<Map<String, Object>, ?>> checks;
    @VisibleForTesting
    final RefLookUpCache refLookUpCache;

    public ValidatedRawInsertSource(DocTableInfo table,
                                    TransactionContext txnCtx,
                                    NodeContext nodeCtx,
                                    String indexName) {
        this.table = table;
        this.inputFactory = new InputFactory(nodeCtx);
        this.txnCtx = txnCtx;
        this.nodeCtx = nodeCtx;
        this.checks = new CheckConstraints<>(
            txnCtx,
            inputFactory,
            new FromSourceRefResolver(table.partitionedByColumns(), indexName),
            table
        );
        this.refLookUpCache = new RefLookUpCache();
    }

    @Override
    public Map<String, Object> generateSourceAndCheckConstraints(Object[] values, List<String> pkValues) throws IOException {
        try {
            String src = (String) values[0];
            Map<String, Object> validatedSource;
            try {
                var parser = JsonXContent.JSON_XCONTENT.createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    new BytesArray(src).array());
                validatedSource = ParameterizedXContentParser.parse(parser, this);
                if (parser.getTokenLocation().columnNumber < src.length()) {
                    throw new IllegalArgumentException(
                        "failed to parse `" + src.substring(parser.getTokenLocation().columnNumber) + "`");
                }
            } catch (JsonParseException e) {
                throw new IllegalArgumentException(
                    e.getMessage().replace("\0", "").replace("\n", ""));
            }

            generateDefaultsAndMerge(validatedSource);

            GeneratedColumns<Map<String, Object>> generatedColumns = new GeneratedColumns<>(
                inputFactory,
                txnCtx,
                true,
                FromSourceRefResolver.WITHOUT_PARTITIONED_BY_REFS,
                refLookUpCache.getPresentColumns(),
                table.generatedColumns());

            generatedColumns.setNextRow(validatedSource);
            generatedColumns.validateValues(validatedSource);

            for (var entry : generatedColumns.generatedToInject()) {
                var reference = entry.getKey();
                var value = entry.getValue().value();
                var valueForInsert = reference
                    .valueType()
                    .valueForInsert(value);
                var column = reference.column();
                Maps.mergeInto(validatedSource, column.name(), column.path(), valueForInsert);
            }

            checks.validate(validatedSource);

            return validatedSource;
        } finally {
            refLookUpCache.clearPresentColumns();
        }
    }

    @Override
    public Object parse(String field, Object value) {
        SimpleReference ref = refLookUpCache.get(field);
        if (ref == null) {
            try {
                ref = table.resolveColumn(field, true, txnCtx.sessionSettings().errorOnUnknownObjectKey());
            } catch (ColumnUnknownException e) {
                // Re-raise as StrictDynamicMappingException to ensure this code path uses the same exception as the DocumentMapper does
                throw new StrictDynamicMappingException(table.ident().toString(), field);
            }
            refLookUpCache.put(field, ref);
        }
        try {
            return ref.valueType().implicitCast(value);
        } catch (Exception e) {
            throw new ConversionException(value, ref.valueType());
        }
    }

    private void generateDefaultsAndMerge(Map<String, Object> source) {
        for (SimpleReference ref : table.defaultExpressionColumns()) {
            ColumnIdent column = ref.column();
            Object val = SymbolEvaluator.evaluateWithoutParams(txnCtx, nodeCtx, ref.defaultExpression());
            Maps.mergeInto(source, column.name(), column.path(), val, Map::putIfAbsent);
        }
    }

    /**
     * {@link RefLookUpCache#lookupCache} caches all refs seen for the entire lifecycle of this instance.
     * <p>
     * {@link RefLookUpCache#presentColumns} caches all refs seen per invocations to {@link #generateSourceAndCheckConstraints}.
     * When going out of scope, presentColumns must be cleared.
     */
    static class RefLookUpCache {

        @VisibleForTesting
        final Map<String, SimpleReference> lookupCache;
        @VisibleForTesting
        List<SimpleReference> presentColumns;

        public RefLookUpCache() {
            this.lookupCache = new HashMap<>();
            this.presentColumns = new ArrayList<>();
        }

        @Nullable
        public SimpleReference get(String col) {
            var r = lookupCache.get(col);
            if (r != null) {
                assert !presentColumns.contains(r) : "for both get() and put(), the references in context should be the first encounter";
                presentColumns.add(r);
            }
            return r;
        }

        public void put(String col, SimpleReference r) {
            assert !lookupCache.containsKey(col) || !lookupCache.containsValue(r) : "the key, value pair already exists";
            assert !presentColumns.contains(r) : "for both get() and put(), the references in context should be the first encounter";
            presentColumns.add(r);
            lookupCache.put(col, r);
        }

        public List<SimpleReference> getPresentColumns() {
            return presentColumns;
        }

        public void clearPresentColumns() {
            presentColumns = new ArrayList<>();
        }
    }
}
