/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.executor.transport.task;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.Task;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides basic upstreamResult/result handling for tasks that return the number of affected rows in their result.
 * This class will chain the results and create the sum of their results or set it to -1 if any of the results returned -1
 *
 * Implementations have to set the result on {@link #result} and just have to implement {@link #start()}.
 */
public abstract class AsyncChainedTask implements Task<Object[][]> {

    protected final SettableFuture<Object[][]> result;
    private final List<ListenableFuture<Object[][]>> resultList;

    protected AsyncChainedTask() {
        result = SettableFuture.create();
        resultList = new ArrayList<>();
        resultList.add(result);
    }

    @Override
    public List<ListenableFuture<Object[][]>> result() {
        if (resultList.size() == 1) {
            // no upstreams.. return result directly
            return resultList;
        }

        // got upstreams, aggregate results together
        final SettableFuture<Object[][]> future = SettableFuture.create();
        Futures.addCallback(Futures.allAsList(resultList), new FutureCallback<List<Object[][]>>() {
            @Override
            public void onSuccess(@Nullable List<Object[][]> results) {
                assert results != null;
                long aggregatedRowsAffected = 0;
                for (Object[][] result : results) {
                    Long rowsAffected = (Long) result[0][0];
                    if (rowsAffected < 0) {
                        aggregatedRowsAffected = rowsAffected;
                        break;
                    } else {
                        aggregatedRowsAffected += rowsAffected;
                    }
                }
                future.set(new Object[][] { new Object[] { aggregatedRowsAffected }});
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                future.setException(t);
            }
        });
        return ImmutableList.of((ListenableFuture<Object[][]>) future);
    }

    @Override
    public void upstreamResult(List<ListenableFuture<Object[][]>> result) {
        resultList.addAll(result);
    }
}
