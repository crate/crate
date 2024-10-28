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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.data.Row;
import io.crate.session.BaseResultReceiver;

class RestBulkRowCountReceiver extends BaseResultReceiver {

    private final Result[] results;
    private final int resultIdx;
    private long rowCount;
    @Nullable
    private Throwable failure;

    RestBulkRowCountReceiver(Result[] results, int resultIdx) {
        this.results = results;
        this.resultIdx = resultIdx;
    }

    @Override
    public void setNextRow(Row row) {
        rowCount = (long) row.get(0);
        failure = (Throwable) row.get(1);
    }

    @Override
    public void allFinished() {
        if (failure == null) {
            results[resultIdx] = new Result(rowCount, null);
        } else {
            results[resultIdx] = new Result(rowCount, failure);
        }
        super.allFinished();
    }

    @Override
    public void fail(@NotNull Throwable t) {
        results[resultIdx] = new Result(rowCount, t);
        super.fail(t);
    }

    record Result(long rowCount, @Nullable Throwable error) {
    }
}
