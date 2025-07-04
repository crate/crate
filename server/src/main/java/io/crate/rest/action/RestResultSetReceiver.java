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

package io.crate.rest.action;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.jetbrains.annotations.Nullable;

import io.crate.session.ResultReceiver;
import io.crate.data.Row;
import io.crate.expression.symbol.Symbol;

class RestResultSetReceiver implements ResultReceiver<XContentBuilder> {

    private final List<Symbol> outputFields;
    private final ResultToXContentBuilder builder;
    private final long startTimeNs;
    private final CompletableFuture<XContentBuilder> result = new CompletableFuture<>();

    private long rowCount;

    RestResultSetReceiver(XContentBuilder builder,
                          List<Symbol> outputFields,
                          List<String> outputFieldNames,
                          long startTimeNs,
                          boolean includeTypesOnResponse) throws IOException {
        this.outputFields = outputFields;
        this.startTimeNs = startTimeNs;
        this.builder = ResultToXContentBuilder.builder(builder);
        this.builder.cols(outputFieldNames);
        if (includeTypesOnResponse) {
            this.builder.colTypes(outputFields);
        }
        this.builder.startRows();
    }

    @Override
    @Nullable
    public CompletableFuture<Void> setNextRow(Row row) {
        try {
            builder.addRow(row, outputFields.size());
            rowCount++;
            return null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void batchFinished() {
        fail(new IllegalStateException("Incremental result streaming not supported via HTTP"));
    }

    @Override
    public void allFinished() {
        try {
            result.complete(finishBuilder());
        } catch (IOException e) {
            result.completeExceptionally(e);
        }
    }

    @Override
    public void fail(Throwable t) {
        result.completeExceptionally(t);
    }

    XContentBuilder finishBuilder() throws IOException {
        return builder
            .finishRows()
            .rowCount(rowCount)
            .duration(startTimeNs)
            .build();
    }

    @Override
    public CompletableFuture<XContentBuilder> completionFuture() {
        return result;
    }
}
