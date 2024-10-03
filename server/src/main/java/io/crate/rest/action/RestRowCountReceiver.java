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
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.common.xcontent.XContentBuilder;

import io.crate.session.ResultReceiver;
import io.crate.data.Row;

class RestRowCountReceiver implements ResultReceiver<XContentBuilder> {

    private final long startTimeNs;
    private final boolean includeTypes;
    private final ResultToXContentBuilder builder;
    private final CompletableFuture<XContentBuilder> result = new CompletableFuture<>();

    private long rowCount;

    RestRowCountReceiver(XContentBuilder builder,
                         long startTimeNs,
                         boolean includeTypes) throws IOException {
        this.startTimeNs = startTimeNs;
        this.includeTypes = includeTypes;
        this.builder = ResultToXContentBuilder.builder(builder);
    }

    @Override
    public void setNextRow(Row row) {
        rowCount = (long) row.get(0);
    }

    @Override
    public void batchFinished() {
        fail(new IllegalStateException("Incremental result streaming not supported via HTTP"));
    }

    XContentBuilder finishBuilder() throws IOException {
        builder.cols(Collections.emptyList());
        if (includeTypes) {
            builder.colTypes(Collections.emptyList());
        }
        builder.startRows()
            .addRow(Row.EMPTY, 0)
            .finishRows()
            .rowCount(rowCount)
            .duration(startTimeNs);
        return builder.build();
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

    @Override
    public CompletableFuture<XContentBuilder> completionFuture() {
        return result;
    }
}
