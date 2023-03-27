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
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder.Writer;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.BitString;

public interface InsertSourceGen {

    public static final Map<Class<?>, Writer> SOURCE_WRITERS = Map.ofEntries(
        Map.entry(BitString.class, (b, v) -> {
            BitString bs = (BitString) v;
            byte[] byteArray = bs.bitSet().toByteArray();
            b.value(byteArray);
        })
    );


    default BytesReference generateSourceAndCheckConstraintsAsBytesReference(Object[] values) throws IOException {
        return BytesReference.bytes(JsonXContent.builder().map(generateSourceAndCheckConstraints(values, List.of()), SOURCE_WRITERS));
    }

    Map<String, Object> generateSourceAndCheckConstraints(Object[] values, List<String> pkValues) throws IOException;

    static InsertSourceGen of(TransactionContext txnCtx,
                              NodeContext nodeCtx,
                              DocTableInfo table,
                              String indexName,
                              boolean validate,
                              List<Reference> targets) {
        if (targets.size() == 1 && targets.get(0).column().equals(DocSysColumns.RAW)) {
            if (!validate && table.generatedColumns().isEmpty() && table.defaultExpressionColumns().isEmpty()) {
                return new RawInsertSource();
            } else {
                return new ValidatedRawInsertSource(table, txnCtx, nodeCtx, indexName);
            }
        }
        throw new UnsupportedOperationException("InsertSourceGen is only available for _raw, Use Indexer for other use-cases");
    }
}
