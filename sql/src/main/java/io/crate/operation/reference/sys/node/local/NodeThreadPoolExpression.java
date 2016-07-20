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

package io.crate.operation.reference.sys.node.local;

import io.crate.metadata.SimpleObjectExpression;
import io.crate.operation.reference.NestedObjectExpression;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.concurrent.XRejectedExecutionHandler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

class NodeThreadPoolExpression extends NestedObjectExpression {

    private static final String POOL_NAME = "name";
    private static final String ACTIVE = "active";
    private static final String REJECTED = "rejected";
    private static final String LARGEST = "largest";
    private static final String COMPLETED = "completed";
    private static final String THREADS = "threads";
    private static final String QUEUE = "queue";

    private final ThreadPoolExecutor threadPoolExecutor;
    private final BytesRef name;

    NodeThreadPoolExpression(ThreadPool threadPool, String name) {
        this.threadPoolExecutor = (ThreadPoolExecutor) threadPool.executor(name);
        this.name = new BytesRef(name);
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(POOL_NAME, new SimpleObjectExpression<BytesRef>() {
            @Override
            public BytesRef value() {
                return name;
            }
        });
        childImplementations.put(ACTIVE, new SimpleObjectExpression<Integer>() {
            @Override
            public Integer value() {
                return threadPoolExecutor.getActiveCount();
            }
        });
        childImplementations.put(REJECTED, new SimpleObjectExpression<Long>() {
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
        childImplementations.put(LARGEST, new SimpleObjectExpression<Integer>() {
            @Override
            public Integer value() {
                return threadPoolExecutor.getLargestPoolSize();
            }
        });
        childImplementations.put(COMPLETED, new SimpleObjectExpression<Long>() {
            @Override
            public Long value() {
                return threadPoolExecutor.getCompletedTaskCount();
            }
        });
        childImplementations.put(THREADS, new SimpleObjectExpression<Integer>() {
            @Override
            public Integer value() {
                return threadPoolExecutor.getPoolSize();
            }
        });
        childImplementations.put(QUEUE, new SimpleObjectExpression<Integer>() {
            @Override
            public Integer value() {
                return threadPoolExecutor.getQueue().size();
            }
        });
    }
}
