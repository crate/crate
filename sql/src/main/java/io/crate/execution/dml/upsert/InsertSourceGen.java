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

import io.crate.metadata.TransactionContext;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.util.List;

public interface InsertSourceGen {

    void checkConstraints(Object[] values);

    BytesReference generateSource(Object[] values) throws IOException;


    static InsertSourceGen of(TransactionContext txnCtx,
                              Functions functions,
                              DocTableInfo table,
                              String indexName,
                              GeneratedColumns.Validation validation,
                              List<Reference> targets) {
        if (targets.size() == 1 && targets.get(0).column().equals(DocSysColumns.RAW)) {
            if (table.generatedColumns().isEmpty() && table.defaultExpressionColumns().isEmpty()) {
                return new FromRawInsertSource();
            } else {
                return new GeneratedColsFromRawInsertSource(txnCtx, functions, table.generatedColumns(), table.defaultExpressionColumns());
            }
        }
        return new InsertSourceFromCells(txnCtx, functions, table, indexName, validation, targets);
    }
}
