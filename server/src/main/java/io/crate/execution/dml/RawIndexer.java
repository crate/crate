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

package io.crate.execution.dml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.lucene.document.FieldType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.ParsedDocument;

import io.crate.exceptions.ConversionException;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.server.xcontent.XContentHelper;
import io.crate.types.DataType;

public class RawIndexer {

    private final String indexName;
    private final DocTableInfo table;
    private final TransactionContext txnCtx;
    private final NodeContext nodeCtx;
    private final Function<ColumnIdent, FieldType> getFieldType;
    private final Symbol[] returnValues;

    private final Map<Set<String>, Indexer> indexers = new HashMap<>();

    public RawIndexer(String indexName,
                      DocTableInfo table,
                      TransactionContext txnCtx,
                      NodeContext nodeCtx,
                      Function<ColumnIdent, FieldType> getFieldType,
                      Symbol[] returnValues) {
        this.indexName = indexName;
        this.table = table;
        this.txnCtx = txnCtx;
        this.nodeCtx = nodeCtx;
        this.getFieldType = getFieldType;
        this.returnValues = returnValues;
    }

    public ParsedDocument index(IndexItem item) throws IOException {
        String raw = (String) item.insertValues()[0];
        Map<String, Object> doc = XContentHelper.convertToMap(JsonXContent.JSON_XCONTENT, raw, true);
        Indexer indexer = indexers.computeIfAbsent(doc.keySet(), keys -> {
            List<Reference> targetRefs = new ArrayList<>();
            for (String key : keys) {
                ColumnIdent column = new ColumnIdent(key);
                Reference reference = table.getReference(column);
                if (reference == null) {
                    reference = table.getDynamic(column, true, txnCtx.sessionSettings().errorOnUnknownObjectKey());
                }
                targetRefs.add(reference);
            }
            return new Indexer(
                indexName,
                table,
                txnCtx,
                nodeCtx,
                getFieldType,
                targetRefs,
                returnValues
            );
        });
        Object[] insertValues = new Object[doc.size()];
        Iterator<Object> iterator = doc.values().iterator();
        List<Reference> columns = indexer.columns();
        for (int i = 0; i < insertValues.length; i++) {
            Reference reference = columns.get(i);
            Object value = iterator.next();
            DataType<?> type = reference.valueType();
            try {
                insertValues[i] = type.implicitCast(value);
            } catch (ClassCastException | IllegalArgumentException e) {
                throw new ConversionException(value, type);
            }
        }
        ParsedDocument parsedDoc = indexer.index(new IndexItem.StaticItem(
            item.id(),
            item.pkValues(),
            insertValues,
            item.seqNo(),
            item.primaryTerm()
        ));
        return parsedDoc;
    }
}
