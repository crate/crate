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

import io.crate.action.sql.ResultReceiver;
import io.crate.breaker.RowAccounting;
import io.crate.data.Row;
import io.crate.expression.symbol.Symbol;
import org.elasticsearch.common.xcontent.XContentBuilder;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

class RestResultSetReceiver implements ResultReceiver<XContentBuilder> {

    private final List<Symbol> outputFields;
    private final ResultToXContentBuilder builder;
    private final long startTimeNs;
    private final RowAccounting<Row> rowAccounting;
    private final CompletableFuture<XContentBuilder> result = new CompletableFuture<>();

    private long rowCount;

    RestResultSetReceiver(XContentBuilder builder,
                          List<Symbol> outputFields,
                          long startTimeNs,
                          RowAccounting<Row> rowAccounting,
                          boolean includeTypesOnResponse) throws IOException {
        this.outputFields = outputFields;
        this.startTimeNs = startTimeNs;
        this.rowAccounting = rowAccounting;
        this.builder = ResultToXContentBuilder.builder(builder);
        this.builder.cols(outputFields);
        if (includeTypesOnResponse) {
            this.builder.colTypes(outputFields);
        }
        this.builder.startRows();
    }

    @Override
    public void setNextRow(Row row) {
        try {
            rowAccounting.accountForAndMaybeBreak(row);
            builder.addRow(row, outputFields.size());
            rowCount++;
        } catch (IOException e) {
            fail(e);
        }
    }

    @Override
    public void batchFinished() {
        allFinished(false);
        //fail(new IllegalStateException("Incremental result streaming not supported via HTTP"));
    }

    @Override
    public void allFinished(boolean interrupted) {
        try {
            result.complete(finishBuilder());
        } catch (IOException e) {
            result.completeExceptionally(e);
        }
    }

    @Override
    public void fail(@Nonnull Throwable t) {
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
