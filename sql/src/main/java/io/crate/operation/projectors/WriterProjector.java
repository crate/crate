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
import io.crate.operation.Input;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.projectors.writer.Output;
import io.crate.operation.projectors.writer.OutputFile;
import io.crate.operation.projectors.writer.OutputS3;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class WriterProjector implements Projector, RowDownstreamHandle {

    private static final byte NEW_LINE = (byte) '\n';

    private final URI uri;
    private final Set<CollectExpression<Row, ?>> collectExpressions;
    private final List<Input<?>> inputs;
    private final Map<String, Object> overwrites;
    private Output output;

    protected final AtomicInteger remainingUpstreams = new AtomicInteger();
    protected final AtomicLong counter = new AtomicLong();
    private final AtomicReference<Throwable> failure = new AtomicReference<>(null);

    private RowDownstreamHandle downstream;
    private RowWriter rowWriter;

    /**
     * @param inputs a list of {@link io.crate.operation.Input}.
     *               If null the row that is received in {@link #setNextRow(Row)}
     *               is expected to contain the raw source in its first column.
     *               That raw source is then written to the output
     *
     *               If inputs is not null the inputs are consumed to write a JSON array to the output.
     */
    public WriterProjector(ExecutorService executorService,
                           String uri,
                           Settings settings,
                           @Nullable List<Input<?>> inputs,
                           Set<CollectExpression<Row, ?>> collectExpressions,
                           Map<ColumnIdent, Object> overwrites) {
        this.collectExpressions = collectExpressions;
        this.inputs = inputs;
        this.overwrites = toNestedStringObjectMap(overwrites);
        try {
            this.uri = new URI(uri);
        } catch (URISyntaxException e) {
            throw new ValidationException(String.format("Invalid uri '%s'", uri), e);
        }
        if (this.uri.getScheme() == null || this.uri.getScheme().equals("file")) {
            this.output = new OutputFile(this.uri, settings);
        } else if (this.uri.getScheme().equalsIgnoreCase("s3")) {
            this.output = new OutputS3(executorService, this.uri, settings);
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
    public void startProjection(ExecutionState executionState) {
        counter.set(0);
        try {
            output.open();
            if (!overwrites.isEmpty()) {
                rowWriter = new DocWriter(
                        output.getOutputStream(), collectExpressions, overwrites, failure);
            } else if (inputs != null && !inputs.isEmpty()) {
                rowWriter = new ColumnRowWriter(output.getOutputStream(), collectExpressions, inputs, failure);
            } else {
                rowWriter = new RawRowWriter(output.getOutputStream(), failure);
            }
        } catch (IOException e) {
            UnhandledServerException t = new UnhandledServerException(
                    String.format("Failed to open output: '%s'", e.getMessage()), e);
            failure.set(t);
        }
    }

    private void endProjection() {
        try {
            if (rowWriter != null) {
                rowWriter.close();
            }
            output.close();
        } catch (IOException e) {
            failure.set(new UnhandledServerException("Failed to close output", e));
        }
        if (downstream != null) {
            Throwable throwable = failure.get();
            if (throwable == null) {
                downstream.setNextRow(new Row1(counter.get()));
                downstream.finish();
            } else {
                downstream.fail(throwable);
            }
        }
    }

    @Override
    public synchronized boolean setNextRow(Row row) {
        if (failure.get() != null) {
            return false;
        }
        rowWriter.write(row);
        counter.incrementAndGet();
        return true;
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        remainingUpstreams.incrementAndGet();
        return this;
    }

    @Override
    public void finish() {
        if (remainingUpstreams.decrementAndGet() == 0) {
            endProjection();
        }
    }

    @Override
    public void fail(Throwable throwable) {
        failure.set(throwable);
        finish();
    }

    @Override
    public void downstream(RowDownstream downstream) {
        this.downstream = downstream.registerUpstream(this);
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume(boolean async) {
        throw new UnsupportedOperationException();
    }


    interface RowWriter {
        void write(Row row);

        void close();
    }

    static class DocWriter implements RowWriter {

        private final OutputStream outputStream;
        private final Set<CollectExpression<Row, ?>> collectExpressions;
        private final Map<String, Object> overwrites;
        private final AtomicReference<Throwable> failure;
        private final XContentBuilder builder;

        public DocWriter(OutputStream outputStream,
                         Set<CollectExpression<Row, ?>> collectExpressions,
                         Map<String, Object> overwrites,
                         AtomicReference<Throwable> failure) throws IOException {
            this.outputStream = outputStream;
            this.collectExpressions = collectExpressions;
            this.overwrites = overwrites;
            this.failure = failure;
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
                failure.set(new UnhandledServerException("Failed to write row to output", e));
            }
        }

        @Override
        public void close() {
        }
    }

    static class RawRowWriter implements RowWriter {

        private final OutputStream outputStream;
        private final AtomicReference<Throwable> failure;

        RawRowWriter(OutputStream outputStream, AtomicReference<Throwable> failure) {
            this.outputStream = outputStream;
            this.failure = failure;
        }

        @Override
        public void write(Row row) {
            BytesRef value = (BytesRef) row.get(0);
            try {
                outputStream.write(value.bytes, value.offset, value.length);
                outputStream.write(NEW_LINE);
            } catch (IOException e) {
                failure.set(new UnhandledServerException("Failed to write row to output", e));
            }
        }

        @Override
        public void close() {

        }
    }

    static class ColumnRowWriter implements RowWriter {
        private final Set<CollectExpression<Row, ?>> collectExpressions;
        private final List<Input<?>> inputs;
        private final AtomicReference<Throwable> failure;
        private final OutputStream outputStream;
        private final XContentBuilder builder;

        ColumnRowWriter(OutputStream outputStream,
                        Set<CollectExpression<Row, ?>> collectExpressions,
                        List<Input<?>> inputs,
                        AtomicReference<Throwable> failure) throws IOException {
            this.outputStream = outputStream;
            this.collectExpressions = collectExpressions;
            this.inputs = inputs;
            this.failure = failure;
            builder = XContentFactory.jsonBuilder(outputStream);
        }

        public void write(Row row) {
            for (CollectExpression<Row, ?> collectExpression : collectExpressions) {
                collectExpression.setNextRow(row);
            }
            try {
                builder.startArray();
                for (Input<?> input : inputs) {
                    builder.value(input.value());
                }
                builder.endArray();
                builder.flush();
                outputStream.write(NEW_LINE);
            } catch (IOException e) {
                failure.set(new UnhandledServerException("Failed to write row to output", e));
            }
        }

        @Override
        public void close() {
            builder.close();
        }
    }
}
