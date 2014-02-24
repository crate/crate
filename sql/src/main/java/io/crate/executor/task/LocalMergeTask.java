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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.Task;
import io.crate.operator.operations.ImplementationSymbolVisitor;
import io.crate.operator.operations.merge.MergeOperation;
import io.crate.planner.node.MergeNode;
import org.cratedb.Constants;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * merging rows locally on the handler
 */
public class LocalMergeTask implements Task<Object[][]> {

    private final ESLogger logger = Loggers.getLogger(getClass());

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
                        boolean last = countDown.decrementAndGet() == 0;
                        Object[][] rows;
                        try {
                            rows = upStreamResult.get();
                            if (logger.isTraceEnabled()) {
                                String result = Joiner.on(", ").join(Collections2.transform(Arrays.asList(rows),
                                        new Function<Object[], String>() {
                                            @Nullable
                                            @Override
                                            public String apply(@Nullable Object[] input) {
                                                return Arrays.toString(input);
                                            }
                                        })
                                );
                                logger.trace(String.format("received result: %s", result));
                            }
                        } catch (Exception e) {
                            logger.error("Failure getting upstream Result: ", e);
                            result.setException(e.getCause());
                            return;
                        }
                        try {
                            mergeOperation.addRows(rows); // TODO: apply timeout
                        } catch (Exception e) {
                            result.setException(e);
                            logger.error("Failed to add rows", e);
                            return;
                        }
                        if (last) {
                            Object[][] mergeResult = null;
                            try {
                                mergeResult = mergeOperation.result();
                            } catch (Exception e) {
                                result.setException(e);
                                logger.error("Failed to get merge result", e);
                                return;
                            }
                            assert mergeResult != null;
                            result.set(mergeResult);
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
