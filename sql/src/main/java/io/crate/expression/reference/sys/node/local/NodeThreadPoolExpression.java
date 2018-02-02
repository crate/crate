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

package io.crate.expression.reference.sys.node.local;

import io.crate.expression.reference.NestedObjectExpression;
import io.crate.expression.reference.sys.shard.LiteralNestableInput;
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
        childImplementations.put(POOL_NAME, new LiteralNestableInput<>(name));
        childImplementations.put(ACTIVE, threadPoolExecutor::getActiveCount);
        childImplementations.put(REJECTED, () -> {
            long rejected = -1;
            RejectedExecutionHandler rejectedExecutionHandler = threadPoolExecutor.getRejectedExecutionHandler();
            if (rejectedExecutionHandler instanceof XRejectedExecutionHandler) {
                rejected = ((XRejectedExecutionHandler) rejectedExecutionHandler).rejected();
            }
            return rejected;
        });
        childImplementations.put(LARGEST, threadPoolExecutor::getLargestPoolSize);
        childImplementations.put(COMPLETED, threadPoolExecutor::getCompletedTaskCount);
        childImplementations.put(THREADS, threadPoolExecutor::getPoolSize);
        childImplementations.put(QUEUE, () -> threadPoolExecutor.getQueue().size());
    }
}
