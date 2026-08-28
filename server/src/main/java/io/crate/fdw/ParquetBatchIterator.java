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

package io.crate.fdw;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;

import io.crate.metadata.Reference;

public class ParquetBatchIterator implements BatchIterator<Row> {

    private static final Logger LOGGER = LogManager.getLogger(ParquetBatchIterator.class);

    private final Object[] cells;
    private final Row row;
    private final List<Reference> columns;
    private final String parquetFilePath;

    public ParquetBatchIterator(String parquetFilePath, List<Reference> columns) {
        this.parquetFilePath = parquetFilePath;
        this.columns = columns;
        this.cells = new Object[columns.size()];
        this.row = new RowN(cells);
    }

    @Override
    public void kill(Throwable throwable) {

    }

    @Override
    public Row currentElement() {
        return row;
    }

    @Override
    public boolean moveNext() {
        return true;
    }

    @Override
    public void moveToStart() {
    }

    @Override
    public void close() {}

    @Override
    public boolean allLoaded() {
        return true;
    }

    @Override
    public boolean hasLazyResultSet() {
        return true;
    }

    @Override
    public CompletionStage<?> loadNextBatch() throws Exception {
        return CompletableFuture.completedFuture(null);
    }


}
