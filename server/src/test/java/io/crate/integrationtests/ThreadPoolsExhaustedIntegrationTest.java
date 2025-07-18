/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.integrationtests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.ArrayList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.testing.SQLResponse;
import io.crate.testing.SQLTransportExecutor;

@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, maxNumDataNodes = 2)
public class ThreadPoolsExhaustedIntegrationTest extends IntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("thread_pool.search.size", 2)
            .put("thread_pool.search.queue_size", 2)
            .build();
    }

    @Test
    public void testRegularSelectWithFewAvailableThreadsShouldNeverGetStuck() throws Exception {
        execute("create table t (x int) with (number_of_replicas = 0)");
        ensureYellow();
        bulkInsert(10);

        assertRejectedExecutionFailure("select * from t limit ?", new Object[]{1000});
    }

    @Test
    public void testDistributedPushSelectWithFewAvailableThreadsShouldNeverGetStuck() throws Exception {
        execute("create table t (x int) with (number_of_replicas = 0)");
        ensureYellow();
        bulkInsert(10);

        // by setting a very high limit we force a push based collection instead of a direct response
        assertRejectedExecutionFailure("select * from t limit ?", new Object[]{1_000_000});
    }

    private void assertRejectedExecutionFailure(String stmt, Object[] parameters) throws Exception {
        ArrayList<CompletableFuture<SQLResponse>> futures = new ArrayList<>();
        ArrayList<Thread> threads = new ArrayList<>();
        int numThreads = 10;
        int reqPerThread = 50;
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        for (int i = 0; i < numThreads; i++) {
            Thread t = new Thread(() -> {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    // ignore
                }
                for (int j = 0; j < reqPerThread; j++) {
                    CompletableFuture<SQLResponse> future = sqlExecutor.execute(stmt, parameters);
                    synchronized (futures) {
                        futures.add(future);
                    }
                }
            });
            t.start();
            threads.add(t);
        }

        for (var thread : threads) {
            thread.join();
        }
        for (CompletableFuture<SQLResponse> future : futures) {
            try {
                future.get(SQLTransportExecutor.REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                fail("query run into a timeout");
            } catch (Exception e) {
                assertThat(e.getMessage()).satisfiesAnyOf(
                    msg -> assertThat(msg).contains("rejected execution"),
                    msg -> assertThat(msg).contains("job killed"));
            }
        }
    }

    private void bulkInsert(int docCount) {
        Object[][] bulkArgs = new Object[docCount][1];
        for (int i = 0; i < docCount; i++) {
            bulkArgs[i][0] = i;
        }
        var bulkResponse = execute("insert into t (x) values (?)", bulkArgs);
        for (int i = 0; i < bulkResponse.size(); i++) {
            assertThat(bulkResponse.rowCount(i)).isEqualTo(1L);
        }
    }
}
