/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.projectors;

import io.crate.core.collections.Row;
import io.crate.core.collections.Row1;
import io.crate.exceptions.UnhandledServerException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.ValidationException;
import io.crate.jobs.ExecutionState;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.projectors.writer.Output;
import io.crate.operation.projectors.writer.OutputFile;
import io.crate.operation.projectors.writer.OutputS3;
import io.crate.planner.projection.WriterProjection;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class WriterProjector extends AbstractProjector {

    private static final byte NEW_LINE = (byte) '\n';

    private final URI uri;
    private final Set<CollectExpression<Row, ?>> collectExpressions;
    private final List<Input<?>> inputs;
    private final Map<String, Object> overwrites;
    @Nullable
    private final List<String> outputNames;
    private final WriterProjection.OutputFormat outputFormat;
    private final WriterProjection.CompressionType compressionType;
    private Output output;

    protected final AtomicLong counter = new AtomicLong();
    private RowWriter rowWriter;

    /**
     * @param inputs a list of {@link io.crate.operation.Input}.
     *               If null the row that is received in {@link #setNextRow(Row)}
     *               is expected to contain the raw source in its first column.
     *               That raw source is then written to the output
     *               <p/>
     *               If inputs is not null the inputs are consumed to write a JSON array to the output.
     */
    public WriterProjector(ExecutorService executorService,
                           String uri,
                           @Nullable WriterProjection.CompressionType compressionType,
                           @Nullable List<Input<?>> inputs,
                           Set<CollectExpression<Row, ?>> collectExpressions,
                           Map<ColumnIdent, Object> overwrites,
                           @Nullable List<String> outputNames,
                           WriterProjection.OutputFormat outputFormat) {
        this.collectExpressions = collectExpressions;
        this.inputs = inputs;
        this.overwrites = toNestedStringObjectMap(overwrites);
        this.outputNames = outputNames;
        this.outputFormat = outputFormat;
        this.compressionType = compressionType;
        try {
            this.uri = new URI(uri);
        } catch (URISyntaxException e) {
            throw new ValidationException(String.format("Invalid uri '%s'", uri), e);
        }
        if (this.uri.getScheme() == null || this.uri.getScheme().equals("file")) {
            this.output = new OutputFile(this.uri, this.compressionType);
        } else if (this.uri.getScheme().equalsIgnoreCase("s3")) {
            this.output = new OutputS3(executorService, this.uri, this.compressionType);
        } else {
            throw new UnsupportedFeatureException(String.format("Unknown scheme '%s'", this.uri.getScheme()));
        }
    }

    protected static Map<String, Object> toNestedStringObjectMap(Map<ColumnIdent, Object> columnIdentObjectMap) {
        Map<String, Object> nestedMap = new HashMap<>();
        Map<String, Object> parent = nestedMap;

        for (Map.Entry<ColumnIdent, Object> entry : columnIdentObjectMap.entrySet()) {
            ColumnIdent key = entry.getKey();
            Object value = entry.getValue();

            if (key.path().isEmpty()) {
                nestedMap.put(key.name(), value);
            } else {
                LinkedList<String> path = new LinkedList<>(key.path());
                path.add(0, key.name());

                while (true) {
                    String currentKey = path.pop();
                    if (path.isEmpty()) {
                        parent.put(currentKey, value);
                        break;
                    }

                    Object o = parent.get(currentKey);
                    if (o == null) {
                        Map<String, Object> child = new HashMap<>();
                        parent.put(currentKey, child);
                        parent = child;
                    } else {
                        assert o instanceof Map;
                        parent = (Map) o;
                    }
                }
            }
        }

        return nestedMap;
    }

    @Override
    public void prepare(ExecutionState executionState) {
        counter.set(0);
        try {
            if (!overwrites.isEmpty()) {
                rowWriter = new DocWriter(
                        output.acquireOutputStream(), collectExpressions, overwrites);
            } else if (outputFormat.equals(WriterProjection.OutputFormat.JSON_ARRAY)) {
                rowWriter = new ColumnRowWriter(output.acquireOutputStream(), collectExpressions, inputs);
            } else if (outputNames != null && outputFormat.equals(WriterProjection.OutputFormat.JSON_OBJECT)) {
                rowWriter = new ColumnRowObjectWriter(output.acquireOutputStream(), collectExpressions, inputs, outputNames);
            } else {
                rowWriter = new RawRowWriter(output.acquireOutputStream());
            }
        } catch (IOException e) {
            throw new UnhandledServerException(String.format("Failed to open output: '%s'", e.getMessage()), e);
        }
    }

    @Override
    public boolean setNextRow(Row row) {
        rowWriter.write(row);
        counter.incrementAndGet();
        return true;
    }

    @Override
    public void finish() {
        if (closeWriterAndOutput()) return;

        downstream.setNextRow(new Row1(counter.get()));
        downstream.finish();
    }

    private boolean closeWriterAndOutput() {
        try {
            if (rowWriter != null) {
                rowWriter.close();
            }
        } catch (IOException e) {
            downstream.fail(new UnhandledServerException("Failed to close output", e));
            return true;
        }
        return false;
    }

    @Override
    public void fail(Throwable throwable) {
        if (closeWriterAndOutput()) return;

        downstream.fail(throwable);
    }

    interface RowWriter {

        void write(Row row);

        void close() throws IOException;
    }

    static class DocWriter implements RowWriter {

        private final OutputStream outputStream;
        private final Set<CollectExpression<Row, ?>> collectExpressions;
        private final Map<String, Object> overwrites;
        private final XContentBuilder builder;

        public DocWriter(OutputStream outputStream,
                         Set<CollectExpression<Row, ?>> collectExpressions,
                         Map<String, Object> overwrites) throws IOException {
            this.outputStream = outputStream;
            this.collectExpressions = collectExpressions;
            this.overwrites = overwrites;
            builder = XContentFactory.jsonBuilder(outputStream);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void write(Row row) {
            for (CollectExpression<Row, ?> collectExpression : collectExpressions) {
                collectExpression.setNextRow(row);
            }
            Map doc = (Map) row.get(0);
            XContentHelper.update(doc, overwrites, false);
            try {
                builder.map(doc);
                builder.flush();
                outputStream.write(NEW_LINE);
            } catch (IOException e) {
                throw new UnhandledServerException("Failed to write row to output", e);
            }
        }

        @Override
        public void close() throws IOException {
            outputStream.close();
        }
    }

    static class RawRowWriter implements RowWriter {

        private final OutputStream outputStream;

        RawRowWriter(OutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Override
        public void write(Row row) {
            BytesRef value = (BytesRef) row.get(0);
            try {
                outputStream.write(value.bytes, value.offset, value.length);
                outputStream.write(NEW_LINE);
            } catch (IOException e) {
                throw new UnhandledServerException("Failed to write row to output", e);
            }
        }

        @Override
        public void close() throws IOException {
            outputStream.close();
        }
    }

    static class ColumnRowWriter implements RowWriter {

        private final Set<CollectExpression<Row, ?>> collectExpressions;
        private final OutputStream outputStream;
        protected final List<Input<?>> inputs;
        protected final XContentBuilder builder;

        ColumnRowWriter(OutputStream outputStream,
                        Set<CollectExpression<Row, ?>> collectExpressions,
                        List<Input<?>> inputs) throws IOException {
            this.outputStream = outputStream;
            this.collectExpressions = collectExpressions;
            this.inputs = inputs;
            builder = XContentFactory.jsonBuilder(outputStream);
        }

        public void write(Row row) {
            for (CollectExpression<Row, ?> collectExpression : collectExpressions) {
                collectExpression.setNextRow(row);
            }
            try {
                processInputs();
                builder.flush();
                outputStream.write(NEW_LINE);
            } catch (IOException e) {
                throw new UnhandledServerException("Failed to write row to output", e);
            }
        }

        @Override
        public void close() throws IOException {
            builder.close();
            outputStream.close();
        }

        protected void processInputs() throws IOException {
            builder.startArray();
            for (Input<?> input : inputs) {
                builder.value(input.value());
            }
            builder.endArray();
        }
    }

    static class ColumnRowObjectWriter extends ColumnRowWriter {

        private final List<String> outputNames;

        public ColumnRowObjectWriter(OutputStream outputStream,
                                     Set<CollectExpression<Row, ?>> collectExpressions,
                                     List<Input<?>> inputs,
                                     List<String> outputNames) throws IOException {
            super(outputStream, collectExpressions, inputs);
            this.outputNames = outputNames;
        }

        @Override
        protected void processInputs() throws IOException {
            try {
                builder.startObject();
                for (int i = 0; i < inputs.size(); i++) {
                    builder.field(outputNames.get(i), inputs.get(i).value());
                }
                builder.endObject();
            } catch (IOException e) {
                throw new UnhandledServerException("Failed to write row to output", e);
            }
        }
    }
}
