/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.elasticsearch.search.profile.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class QueryProfilerTest extends ESTestCase {

    private ExecutorService executor;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        executor = Executors.newFixedThreadPool(20);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        executor.shutdown();
        executor.awaitTermination(500, TimeUnit.MILLISECONDS);
        super.tearDown();
    }

    @Test
    public void test_ensure_thread_safety() throws Exception {
        QueryProfiler profiler = new QueryProfiler();
        final AtomicReference<Throwable> lastThrowable = new AtomicReference<>();

        int concurrency = 20;

        final CountDownLatch writeLatch = new CountDownLatch(concurrency);
        for (int i = 0; i < concurrency; i++) {
            executor.submit(() -> {
                try {
                    profiler.getQueryBreakdown(new MatchAllDocsQuery());
                } catch (Exception e) {
                    lastThrowable.set(e);
                } finally {
                    writeLatch.countDown();
                }
            });
        }
        writeLatch.await(10, TimeUnit.SECONDS);

        assertThat(lastThrowable.get()).isNull();

        final CountDownLatch readLatch = new CountDownLatch(concurrency);
        for (int i = 0; i < concurrency; i++) {
            executor.submit(() -> {
                try {
                    profiler.getTree();
                } catch (Exception e) {
                    lastThrowable.set(e);
                } finally {
                    readLatch.countDown();
                }
            });
        }
        readLatch.await(10, TimeUnit.SECONDS);

        assertThat(lastThrowable.get()).isNull();
    }
}
