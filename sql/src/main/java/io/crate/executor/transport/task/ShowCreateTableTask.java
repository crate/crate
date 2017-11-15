/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport.task;

import io.crate.analyze.MetaDataToASTNodeResolver;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.executor.Task;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.SqlFormatter;
import io.crate.sql.tree.CreateTable;
import org.apache.lucene.util.BytesRef;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.crate.data.SentinelRow.SENTINEL;

public class ShowCreateTableTask implements Task {

    private final DocTableInfo tableInfo;

    public ShowCreateTableTask(DocTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    @Override
    public void execute(RowConsumer consumer, Row parameters) {
        Row1 row;
        try {
            CreateTable createTable = MetaDataToASTNodeResolver.resolveCreateTable(tableInfo);
            row = new Row1(new BytesRef(SqlFormatter.formatSql(createTable)));
        } catch (Throwable t) {
            consumer.accept(null, t);
            return;
        }
        consumer.accept(InMemoryBatchIterator.of(row, SENTINEL), null);
    }

    @Override
    public List<CompletableFuture<Long>> executeBulk(List<Row> bulkParams) {
        throw new UnsupportedOperationException("show task cannot be executed as bulk operation");
    }
}
