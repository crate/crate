/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.indices;

import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static org.hamcrest.Matchers.equalTo;

public class RefreshThreadPoolTest extends IndexShardTestCase {

    public void test_refresh_executor_directly() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) threadPool.executor(ThreadPool.Names.REFRESH);
        for(int i = 5; i > 0; i--) {
            var msg = "Execute " + i;
            executor.execute(() -> System.out.println(msg));
        }
        assertBusy(() -> assertThat(executor.getCompletedTaskCount(), equalTo(5L)));
        executor.shutdown();
    }

    public void test_refresh_plain() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        for (int i = 5; i > 0; i--) {
            var msg = "Execute " + i;
            executor.execute(() -> System.out.println(msg));
        }
        assertBusy(() -> assertThat(executor.getCompletedTaskCount(), equalTo(5L)));
        executor.shutdown();
    }

}
