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

package io.crate.execution.engine.export;

import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.metadata.ColumnIdent;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

public class FileWriterProjector implements Projector {

    private final String uri;
    private final Iterable<CollectExpression<Row, ?>> collectExpressions;
    private final List<Input<?>> inputs;
    private final Map<ColumnIdent, Object> overwrites;
    @Nullable
    private final List<String> outputNames;
    private final WriterProjection.OutputFormat outputFormat;
    private final WriterProjection.CompressionType compressionType;
    private final Executor executor;

    /**
     * @param inputs a list of {@link Input}.
     *               If null the row that is exposed in the BatchIterator
     *               is expected to contain the raw source in its first column.
     *               That raw source is then written to the output
     *               <p/>
     *               If inputs is not null the inputs are consumed to write a JSON array to the output.
     */
    public FileWriterProjector(Executor executor,
                               String uri,
                               @Nullable WriterProjection.CompressionType compressionType,
                               @Nullable List<Input<?>> inputs,
                               Iterable<CollectExpression<Row, ?>> collectExpressions,
                               Map<ColumnIdent, Object> overwrites,
                               @Nullable List<String> outputNames,
                               WriterProjection.OutputFormat outputFormat) {
        this.collectExpressions = collectExpressions;
        this.executor = executor;
        this.inputs = inputs;
        this.overwrites = overwrites;
        this.outputNames = outputNames;
        this.outputFormat = outputFormat;
        this.compressionType = compressionType;
        this.uri = uri;
    }

    @Override
    public BatchIterator<Row> apply(BatchIterator<Row> batchIterator) {
        return CollectingBatchIterator.newInstance(
            batchIterator,
            new FileWriterCountCollector(
                executor,
                uri,
                compressionType,
                inputs,
                collectExpressions,
                overwrites,
                outputNames,
                outputFormat
            )
        );
    }

    @Override
    public boolean providesIndependentScroll() {
        return false;
    }
}
