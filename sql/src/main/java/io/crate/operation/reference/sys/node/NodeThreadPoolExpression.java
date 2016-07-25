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

import io.crate.monitor.ThreadPools;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;


public class NodeThreadPoolExpression extends NestedDiscoveryNodeExpression {

    /**
     * Marker class for child implementations
     */
    private abstract class ThreadPoolExpression<T> extends SimpleDiscoveryNodeExpression<T> {
    }

    private static final String POOL_NAME = "name";
    private static final String ACTIVE = "active";
    private static final String REJECTED = "rejected";
    private static final String LARGEST = "largest";
    private static final String COMPLETED = "completed";
    private static final String THREADS = "threads";
    private static final String QUEUE = "queue";

    private final String name;
    private ThreadPools.ThreadPoolExecutorContext threadPoolExecutor;

    public NodeThreadPoolExpression(String name) {
        this.name = name;
        addChildImplementations();
    }

    @Override
    public void setNextRow(DiscoveryNodeContext row) {
        super.setNextRow(row);
        threadPoolExecutor = this.row.threadPools.get(name);
    }

    private void addChildImplementations() {
        childImplementations.put(POOL_NAME, new ThreadPoolExpression<BytesRef>() {
            @Override
            public BytesRef innerValue() {
                return BytesRefs.toBytesRef(name);
            }
        });
        childImplementations.put(ACTIVE, new ThreadPoolExpression<Integer>() {
            @Override
            public Integer innerValue() {
                return threadPoolExecutor.activeCount();
            }
        });
        childImplementations.put(REJECTED, new ThreadPoolExpression<Long>() {
            @Override
            public Long innerValue() {
                return threadPoolExecutor.rejectedCount();
            }
        });
        childImplementations.put(LARGEST, new ThreadPoolExpression<Integer>() {
            @Override
            public Integer innerValue() {
                return threadPoolExecutor.largestPoolSize();
            }
        });
        childImplementations.put(COMPLETED, new ThreadPoolExpression<Long>() {
            @Override
            public Long innerValue() {
                return threadPoolExecutor.completedTaskCount();
            }
        });
        childImplementations.put(THREADS, new ThreadPoolExpression<Integer>() {
            @Override
            public Integer innerValue() {
                return threadPoolExecutor.poolSize();
            }
        });
        childImplementations.put(QUEUE, new ThreadPoolExpression<Integer>() {
            @Override
            public Integer innerValue() {
                return threadPoolExecutor.queueSize();
            }
        });
    }

}
