/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution;

import io.crate.core.collections.ArrayBucket;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RowReceiver implements Receiver<Row> {

    private final List<Object[]> rows = new ArrayList<>();
    private final CompletableFuture<Bucket> result = new CompletableFuture<Bucket>();

    private int numRows = 0;

    @Override
    public Result<Row> onNext(Row item) {
        numRows++;
        rows.add(item.materialize());
        /*
        if (numRows == 3) {
            return new Result<Row>() {
                @Override
                public Type type() {
                    return Type.SUSPEND;
                }

                @Override
                public CompletableFuture<ResumeHandle> continuation() {
                    return CompletableFuture.completedFuture(() -> {});
                }
            };
        }
        */
        return Result.getContinue();
    }

    @Override
    public void onFinish() {
        result.complete(new ArrayBucket(rows.toArray(new Object[0][])));
    }

    @Override
    public void onError(Throwable t) {
        result.completeExceptionally(t);
    }

    public Bucket result() throws InterruptedException, ExecutionException, TimeoutException {
        return result.get(30, TimeUnit.SECONDS);
    }
}
