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

package io.crate.executor;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.task.LocalAggregationTask;
import io.crate.metadata.Functions;
import io.crate.planner.plan.AggregationNode;

import java.util.List;

public class TestingAggregationTask extends LocalAggregationTask {

    private final SettableFuture<Object[][]> result = SettableFuture.create();
    List<ListenableFuture<Object[][]>> results = ImmutableList.of((ListenableFuture<Object[][]>) result);

    public TestingAggregationTask(AggregationNode node, Functions functions) {
        super(node, functions);
    }

    @Override
    public void start() {
        startCollect();
        for (final ListenableFuture<Object[][]> f : upstreamResults) {
            f.addListener(new Runnable() {

                @Override
                public void run() {
                    Object[][] value = null;
                    try {
                        value = f.get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (java.util.concurrent.ExecutionException e) {
                        e.printStackTrace();
                    }
                    processUpstreamResult(value);
                }
            }, MoreExecutors.sameThreadExecutor());
        }
        result.set(finishCollect());
    }

    @Override
    public List<ListenableFuture<Object[][]>> result() {
        return results;
    }


}
