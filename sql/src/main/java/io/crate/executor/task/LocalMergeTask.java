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

package io.crate.executor.task;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.Task;
import io.crate.operator.operations.ImplementationSymbolVisitor;
import io.crate.operator.operations.merge.MergeOperation;
import io.crate.planner.node.MergeNode;
import org.cratedb.Constants;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * merging rows locally on the handler
 */
public class LocalMergeTask implements Task<Object[][]> {

    private final MergeNode mergeNode;
    private final ImplementationSymbolVisitor symbolVisitor;
    private final ThreadPool threadPool;
    private final SettableFuture<Object[][]> result;
    private final List<ListenableFuture<Object[][]>> resultList;

    private List<ListenableFuture<Object[][]>> upstreamResults;

    /**
     *
     * @param implementationSymbolVisitor symbol visitor (on cluster level)
     * @param mergeNode
     */
    public LocalMergeTask(ThreadPool threadPool, ImplementationSymbolVisitor implementationSymbolVisitor, MergeNode mergeNode) {
        this.threadPool = threadPool;
        this.symbolVisitor = implementationSymbolVisitor;
        this.mergeNode = mergeNode;
        this.result = SettableFuture.create();
        this.resultList = Arrays.<ListenableFuture<Object[][]>>asList(this.result);
    }

    /**
     * only operate if we have an upstream as data always comes in via upstream results
     *
     * start listener for every upStream Result
     * if last listener is done he sets the local result.
     */
    @Override
    public void start() {
        if (upstreamResults != null) {
            final MergeOperation mergeOperation = new MergeOperation(symbolVisitor, mergeNode);
            final AtomicInteger countDown = new AtomicInteger(upstreamResults.size());
            for (final ListenableFuture<Object[][]> upStreamResult : upstreamResults) {
                upStreamResult.addListener(new Runnable() {
                    @Override
                    public void run() {
                        Object[][] upstreamResult;
                        try {
                            upstreamResult = upStreamResult.get();
                        } catch (Exception e) {
                            result.setException(e.getCause());
                            return;
                        };
                        mergeOperation.addRows(upstreamResult);
                        if (countDown.decrementAndGet()==0) {
                            result.set(mergeOperation.result());
                        }
                    }
                }, threadPool.executor(ThreadPool.Names.GENERIC));
            }
        } else {
            result.set(Constants.EMPTY_RESULT);
        }
    }

    @Override
    public List<ListenableFuture<Object[][]>> result() {
        return resultList;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<Object[][]>> result) {
        upstreamResults = result;
    }
}
