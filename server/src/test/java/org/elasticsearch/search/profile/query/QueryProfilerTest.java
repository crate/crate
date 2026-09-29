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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

        int concurrency = 20;
        // getProfileBreakdown + pollLast
        List<Callable<Object>> tasks = new ArrayList<>(concurrency * 2);
        for (int i = 0; i < concurrency; i++) {
            tasks.add(() -> profiler.getProfileBreakdown(MatchAllDocsQuery.INSTANCE));
            tasks.add(Executors.callable(profiler::pollLast));
        }
        for (var future : executor.invokeAll(tasks, 10, TimeUnit.SECONDS)) {
            future.get();
        }

        // getProfileBreakdown + getTree
        tasks = new ArrayList<>(concurrency * 2);
        for (int i = 0; i < concurrency; i++) {
            tasks.add(() -> profiler.getProfileBreakdown(MatchAllDocsQuery.INSTANCE));
            tasks.add(profiler::getTree);
        }
        for (var future : executor.invokeAll(tasks, 10, TimeUnit.SECONDS)) {
            future.get();
        }
    }
}
