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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.exceptions.ConversionException;
import io.crate.metadata.Reference;

public class ParquetBatchIterator implements BatchIterator<Row> {

    private static final Logger LOGGER = LogManager.getLogger(ParquetBatchIterator.class);

    private final Object[] cells;
    private final Row row;
    private ParquetFileReader reader;
    private RowReader rowReader;
    private final List<Reference> columns;
    private final InputFile inputFile;

    public ParquetBatchIterator(InputFile inputFile, List<Reference> columns) {
        this.inputFile = inputFile;
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
        assert (rowReader != null);
        while (rowReader.hasNext()) {
            rowReader.next();
            for (int i = 0; i < columns.size(); i++) {
                Reference ref = columns.get(i);
                // TODO: do we need something like getObject from ResultSetParser? Something to map between the object
                // from Hardwood/parquet and what Crate uses internally
                // TODO: need a way to know method to call on rowReader (getDouble, or getString or whatever)
                double val = rowReader.getDouble(ref.toString());
                try {
                    cells[i] = ref.valueType().implicitCast(val);
                } catch (ClassCastException | IllegalArgumentException e) {
                    var conversionException = new ConversionException(val, ref.valueType());
                    conversionException.addSuppressed(e);
                    throw conversionException;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public void moveToStart() {
    }

    @Override
    public void close() {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                // Placeholder error
                throw new Error("Could not close: " + e.getMessage());
            }
        }
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
        if (reader == null) {
            reader = ParquetFileReader.open(inputFile);
            rowReader = reader.buildRowReader()
                    .projection(ColumnProjection.columns(columns.stream()
                            .map(ref -> ref.column().fqn())
                            .toArray(String[]::new)))
                    .build();
            System.out.println("Loaded reader");
        }
        return CompletableFuture.completedFuture(null);
    }
}
