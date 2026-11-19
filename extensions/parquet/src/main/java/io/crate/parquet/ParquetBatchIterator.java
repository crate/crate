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

package io.crate.parquet;

import static io.crate.parquet.ParquetToCrate.readColumn;
import static io.crate.parquet.Predicates.createFilterPredicate;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import dev.hardwood.InputFile;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.reader.ParquetFileReader.RowReaderBuilder;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import io.crate.common.exceptions.Exceptions;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.GtOperator;
import io.crate.expression.operator.GteOperator;
import io.crate.expression.operator.LtOperator;
import io.crate.expression.operator.LteOperator;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;

public class ParquetBatchIterator implements BatchIterator<Row> {


    private final Object[] cells;
    private final Row row;
    private ParquetFileReader reader;
    private FileSchema fileSchema;
    private RowReader rowReader;
    private final List<Reference> columns;
    private final Symbol predicates;
    private final List<InputFile> inputFiles;
    private volatile Throwable killed = null;
    public static final Map<String, FilterPredicate.Operator> FILTER_PUSHDOWN_OPERATORS = Map.of(
        EqOperator.NAME, FilterPredicate.Operator.EQ,
        GtOperator.NAME, FilterPredicate.Operator.GT,
        GteOperator.NAME, FilterPredicate.Operator.GT_EQ,
        LtOperator.NAME, FilterPredicate.Operator.LT,
        LteOperator.NAME, FilterPredicate.Operator.LT_EQ
    );

    public ParquetBatchIterator(List<InputFile> inputFiles, List<Reference> columns, Symbol predicates) {
        this.inputFiles = inputFiles;
        this.columns = columns;
        this.predicates = predicates;
        this.cells = new Object[columns.size()];
        this.row = new RowN(cells);
    }

    private FilterPredicate createFilter() {
        if (predicates instanceof Function) {
            Function func = (Function) predicates;
            FilterPredicate.Operator hardwoodOperator = FILTER_PUSHDOWN_OPERATORS.get(func.name());
            if (hardwoodOperator == null) {
                throw new UnsupportedOperationException("Unexpected error: Cannot push predicate down for operator: " + func.name());
            }
            Symbol left = func.arguments().get(0);
            Symbol right = func.arguments().get(1);
            if (left instanceof Reference ref && right instanceof Literal<?> literal) {
                return createFilterPredicate(ref, literal.value(), hardwoodOperator);
            }
        }
        return null;
    }

    private void raiseIfKilled() {
        if (killed != null) {
            Exceptions.rethrowUnchecked(killed);
        }
    }

    @Override
    public void kill(Throwable throwable) {
        killed = throwable;
    }

    @Override
    public Row currentElement() {
        return row;
    }

    @Override
    public boolean moveNext() {
        raiseIfKilled();
        if (rowReader == null) {
            return false;
        }

        if (rowReader.hasNext()) {
            rowReader.next();
            for (int i = 0; i < columns.size(); i++) {
                Reference ref = columns.get(i);
                cells[i] = readColumn(fileSchema, rowReader, ref);
            }
            return true;
        }
        return false;
    }

    private void releaseReader() {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                reader = null;
            }
        }
        if (rowReader != null) {
            rowReader.close();
            rowReader = null;
        }

    }

    @Override
    public void moveToStart() {
        raiseIfKilled();
        releaseReader();
        // Hardwood does not support moving the cursor backwards
        // so we reopen the file to start from the beginning
        initReader();
    }

    @Override
    public void close() {
        killed = BatchIterator.CLOSED;
        releaseReader();
    }

    @Override
    public boolean allLoaded() {
        return reader != null;
    }

    @Override
    public boolean hasLazyResultSet() {
        return true;
    }

    @Override
    public CompletionStage<?> loadNextBatch() throws Exception {
        raiseIfKilled();
        if (reader == null) {
            initReader();
            return CompletableFuture.completedFuture(null);
        }
        throw new IllegalStateException("BatchIterator already loaded");
    }

    private void initReader() {
        try {
            reader = ParquetFileReader.openAll(inputFiles);
            fileSchema = reader.getFileSchema();
            RowReaderBuilder builder = reader.buildRowReader();
            if (!columns.isEmpty()) {
                builder.projection(ColumnProjection.columns(columns.stream()
                    .map(ref -> ref.column().fqn())
                    .toArray(String[]::new)));
            }
            FilterPredicate filter = createFilter();
            if (filter != null) {
                builder.filter(filter);
            }
            rowReader = builder.build();
        } catch (IOException e) {
            Exceptions.rethrowUnchecked(e);
        } catch (UncheckedIOException e) {
            Exceptions.rethrowUnchecked(e.getCause());
        }
    }
}
