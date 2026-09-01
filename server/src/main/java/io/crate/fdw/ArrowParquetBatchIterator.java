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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.arrow.dataset.scanner.ScanOptions;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.metadata.Reference;

public class ArrowParquetBatchIterator implements BatchIterator<Row> {
    private static final Logger LOGGER = LogManager.getLogger(ParquetBatchIterator.class);

    private final Object[] cells;
    private final Row row;
    private final List<Reference> columns;
    private final Path parquetFile;
    private final ScanOptions options = new ScanOptions(32768);

    public ArrowParquetBatchIterator(Path parquetFile, List<Reference> columns) {
        this.parquetFile = parquetFile;
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
        return false;
    }

    @Override
    public void moveToStart() {
    }

    @Override
    public void close() {
    }

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
        try (
            BufferAllocator allocator = new RootAllocator();
            DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                        allocator, NativeMemoryPool.getDefault(),
                        FileFormat.PARQUET, parquetFile.toUri().toString());
            Dataset dataset = datasetFactory.finish();
            Scanner scanner = dataset.newScan(options);
            ArrowReader reader = scanner.scanBatches()) {
            List<ArrowRecordBatch> batches = new ArrayList<>();
            while (reader.loadNextBatch()) {
                try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
                    final VectorUnloader unloader = new VectorUnloader(root);
                    batches.add(unloader.getRecordBatch());
                }
            }

            System.out.println(batches);


            // finished the analysis of the data, close all resources:
            AutoCloseables.close(batches);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return CompletableFuture.completedFuture(null);
    }

}
