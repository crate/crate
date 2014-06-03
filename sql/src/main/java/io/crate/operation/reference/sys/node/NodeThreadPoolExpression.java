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
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.reference.sys.node;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.ColumnIdent;
import io.crate.operation.reference.sys.SysNodeObjectReference;
import org.elasticsearch.common.util.concurrent.XRejectedExecutionHandler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

public class NodeThreadPoolExpression extends SysNodeObjectReference {

    public static final String NAME = NodeThreadPoolsExpression.NAME;

    abstract class ThreadPoolExpression<ChildType> extends SysNodeExpression<ChildType> {
        ThreadPoolExpression(String name) {
            super(new ColumnIdent(NAME, ImmutableList.of(name)));
        }
    }

    public static final String POOL_NAME = "name";
    public static final String ACTIVE = "active";
    public static final String REJECTED = "rejected";
    public static final String LARGEST = "largest";
    public static final String COMPLETED = "completed";
    public static final String THREADS = "threads";
    public static final String QUEUE = "queue";

    private final ThreadPoolExecutor threadPoolExecutor;
    private final String name;

    public NodeThreadPoolExpression(ThreadPool threadPool, String name) {
        super(NAME);
        this.threadPoolExecutor = (ThreadPoolExecutor) threadPool.executor(name);
        this.name = name;
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(POOL_NAME, new ThreadPoolExpression<String>(POOL_NAME) {
            @Override
            public String value() {
                return name;
            }
        });
        childImplementations.put(ACTIVE, new ThreadPoolExpression<Integer>(ACTIVE) {
            @Override
            public Integer value() {
                return threadPoolExecutor.getActiveCount();
            }
        });
        childImplementations.put(REJECTED, new ThreadPoolExpression<Long>(REJECTED) {
            @Override
            public Long value() {
                long rejected = -1;
                RejectedExecutionHandler rejectedExecutionHandler = threadPoolExecutor.getRejectedExecutionHandler();
                if (rejectedExecutionHandler instanceof XRejectedExecutionHandler) {
                    rejected = ((XRejectedExecutionHandler) rejectedExecutionHandler).rejected();
                }
                return rejected;
            }
        });
        childImplementations.put(LARGEST, new ThreadPoolExpression<Integer>(LARGEST) {
            @Override
            public Integer value() {
                return threadPoolExecutor.getLargestPoolSize();
            }
        });
        childImplementations.put(COMPLETED, new ThreadPoolExpression<Long>(COMPLETED) {
            @Override
            public Long value() {
                return threadPoolExecutor.getCompletedTaskCount();
            }
        });
        childImplementations.put(THREADS, new ThreadPoolExpression<Integer>(THREADS) {
            @Override
            public Integer value() {
                return threadPoolExecutor.getPoolSize();
            }
        });
        childImplementations.put(QUEUE, new ThreadPoolExpression<Integer>(QUEUE) {
            @Override
            public Integer value() {
                return threadPoolExecutor.getQueue().size();
            }
        });
    }

}