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

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.jetbrains.annotations.Nullable;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.fasterxml.jackson.core.JsonGenerator;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.exceptions.SQLParseException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.metadata.ColumnIdent;
import io.crate.server.xcontent.XContentHelper;

/**
 * Collector implementation which writes the rows to the configured {@link FileOutput}
 * and returns a count representing the number of written rows
 */
public class FileWriterCountCollector implements Collector<Row, long[], Iterable<Row>> {

    private static final byte NEW_LINE = (byte) '\n';

    private final Executor executor;
    private final Iterable<CollectExpression<Row, ?>> collectExpressions;
    private final List<Input<?>> inputs;
    private final URI uri;
    private final FileOutput fileOutput;
    private final Map<String, Object> overwrites;
    private final WriterProjection.CompressionType compressionType;
    @Nullable
    private final List<String> outputNames;
    private final WriterProjection.OutputFormat outputFormat;

    private final RowWriter rowWriter;

    FileWriterCountCollector(Executor executor,
                             String uriStr,
                             @Nullable WriterProjection.CompressionType compressionType,
                             @Nullable List<Input<?>> inputs,
                             Iterable<CollectExpression<Row, ?>> collectExpressions,
                             Map<ColumnIdent, Object> overwrites,
                             @Nullable List<String> outputNames,
                             WriterProjection.OutputFormat outputFormat,
                             Map<String, FileOutputFactory> fileOutputFactories,
                             Settings withClauseOptions) {
        this.executor = executor;
        this.collectExpressions = collectExpressions;
        this.inputs = inputs;
        this.overwrites = toNestedStringObjectMap(overwrites);
        this.compressionType = compressionType;
        this.outputNames = outputNames;
        this.outputFormat = outputFormat;
        try {
            uri = new URI(uriStr);
        } catch (URISyntaxException e) {
            throw new SQLParseException(String.format(Locale.ENGLISH, "Invalid uri '%s'", uriStr), e);
        }
        String scheme = uri.getScheme();
        scheme = (scheme == null) ? LocalFsFileOutputFactory.NAME : scheme;
        FileOutputFactory fileOutputFactory = fileOutputFactories.get(scheme);
        if (fileOutputFactory == null) {
            throw new UnsupportedFeatureException(String.format(Locale.ENGLISH, "Unknown scheme '%s'", scheme));
        }
        fileOutput = fileOutputFactory.create(withClauseOptions);
        this.rowWriter = initWriter();
    }

    @VisibleForTesting
    static Map<String, Object> toNestedStringObjectMap(Map<ColumnIdent, Object> columnIdentObjectMap) {
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
                        assert o instanceof Map : "o must be instance of Map";
                        parent = (Map) o;
                    }
                }
            }
        }

        return nestedMap;
    }

    private RowWriter initWriter() {
        try {
            if (!overwrites.isEmpty()) {
                return new DocWriter(
                    fileOutput.acquireOutputStream(executor, uri, compressionType), collectExpressions, overwrites);
            } else if (outputFormat.equals(WriterProjection.OutputFormat.JSON_ARRAY)) {
                return new ColumnRowWriter(fileOutput.acquireOutputStream(executor, uri, compressionType), collectExpressions, inputs);
            } else if (outputNames != null && outputFormat.equals(WriterProjection.OutputFormat.JSON_OBJECT)) {
                return new ColumnRowObjectWriter(fileOutput.acquireOutputStream(executor, uri, compressionType), collectExpressions, inputs, outputNames);
            } else {
                return new RawRowWriter(fileOutput.acquireOutputStream(executor, uri, compressionType));
            }
        } catch (IOException e) {
            throw new UnhandledServerException(String.format(Locale.ENGLISH, "Failed to open output: '%s'", e.getMessage()), e);
        }
    }

    private void closeWriterAndOutput() {
        try {
            if (rowWriter != null) {
                rowWriter.close();
            }
        } catch (IOException ignored) {

        }
    }

    @Override
    public Supplier<long[]> supplier() {
        return () -> new long[1];
    }

    @Override
    public BiConsumer<long[], Row> accumulator() {
        return this::onNextRow;
    }

    private void onNextRow(long[] container, Row row) {
        rowWriter.write(row);
        container[0] += 1;
    }

    @Override
    public BinaryOperator<long[]> combiner() {
        return (state1, state2) -> {
            throw new UnsupportedOperationException("combine not supported");
        };
    }

    @Override
    public Function<long[], Iterable<Row>> finisher() {
        return (container) -> {
            closeWriterAndOutput();
            return Collections.singletonList(new Row1(container[0]));
        };
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }

    @VisibleForTesting
    static XContentBuilder createJsonBuilder(OutputStream outputStream) throws IOException {
        XContentBuilder builder = XContentFactory.json(outputStream);
        builder.generator().configure(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM, false);
        return builder;
    }

    interface RowWriter {

        void write(Row row);

        void close() throws IOException;
    }

    static class DocWriter implements RowWriter {

        private final OutputStream outputStream;
        private final Iterable<CollectExpression<Row, ?>> collectExpressions;
        private final Map<String, Object> overwrites;
        private final XContentBuilder builder;

        public DocWriter(OutputStream outputStream,
                         Iterable<CollectExpression<Row, ?>> collectExpressions,
                         Map<String, Object> overwrites) throws IOException {
            this.outputStream = outputStream;
            this.collectExpressions = collectExpressions;
            this.overwrites = overwrites;
            builder = createJsonBuilder(outputStream);
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
            String value = (String) row.get(0);
            try {
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                outputStream.write(bytes);
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

        private final Iterable<CollectExpression<Row, ?>> collectExpressions;
        private final OutputStream outputStream;
        protected final List<Input<?>> inputs;
        protected final XContentBuilder builder;

        ColumnRowWriter(OutputStream outputStream,
                        Iterable<CollectExpression<Row, ?>> collectExpressions,
                        List<Input<?>> inputs) throws IOException {
            this.outputStream = outputStream;
            this.collectExpressions = collectExpressions;
            this.inputs = inputs;
            builder = createJsonBuilder(outputStream);
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
                                     Iterable<CollectExpression<Row, ?>> collectExpressions,
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
