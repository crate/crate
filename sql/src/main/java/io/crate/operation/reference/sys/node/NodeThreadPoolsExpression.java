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

package io.crate.operation.reference.sys.node;

import com.google.common.collect.Lists;
import io.crate.monitor.ThreadPools;
import io.crate.operation.reference.sys.ArrayTypeRowContextCollectorExpression;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class NodeThreadPoolsExpression
    extends ArrayTypeRowContextCollectorExpression<NodeStatsContext, Map.Entry<BytesRef, ThreadPools.ThreadPoolExecutorContext>, Object> {

    private static final String POOL_NAME = "name";
    private static final String ACTIVE = "active";
    private static final String REJECTED = "rejected";
    private static final String LARGEST = "largest";
    private static final String COMPLETED = "completed";
    private static final String THREADS = "threads";
    private static final String QUEUE = "queue";

    public NodeThreadPoolsExpression() {
    }

    @Override
    protected List<Map.Entry<BytesRef, ThreadPools.ThreadPoolExecutorContext>> items() {
        return Lists.newArrayList(this.row.threadPools());
    }

    @Override
    protected Object valueForItem(final Map.Entry<BytesRef, ThreadPools.ThreadPoolExecutorContext> input) {
        return new HashMap<String, Object>() {
            {
                put(POOL_NAME, BytesRefs.toBytesRef(input.getKey()));
                put(ACTIVE, input.getValue().activeCount());
                put(COMPLETED, input.getValue().completedTaskCount());
                put(REJECTED, input.getValue().rejectedCount());
                put(LARGEST, input.getValue().largestPoolSize());
                put(QUEUE, input.getValue().queueSize());
                put(THREADS, input.getValue().poolSize());
            }
        };
    }

    @Override
    public Object[] value() {
        return row.isComplete() ? super.value() : null;
    }
}
