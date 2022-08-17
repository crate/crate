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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConcurrencyIntegrationTest extends IntegTestCase {

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
    public void testInsertStatementsDoNotShareState() throws Throwable {
        execute("create table t1 (id int primary key, x long) with (number_of_replicas = 0)");
        execute("create table t2 (id int primary key, x string) with (number_of_replicas = 0)");
        execute("create table t3 (x timestamp with time zone) with (number_of_replicas = 0)");
        execute("create table t4 (y string) with (number_of_replicas = 0)");

        final CountDownLatch latch = new CountDownLatch(1000);
        final AtomicReference<Throwable> lastThrowable = new AtomicReference<>();

        String[] statements = new String[]{
            "insert into t1 (id, x) values (1, 10) on conflict (id) do update set x = x + 10 ",
            "insert into t2 (id, x) values (1, 'bar') on conflict (id) do update set x = 'foo' ",
            "insert into t3 (x) values (current_timestamp) ",
            "insert into t4 (y) values ('foo') ",
        };

        // run every statement 5 times, so all will fit into the executors pool
        for (final String statement : statements) {
            for (int i = 0; i < 5; i++) {
                executor.submit(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                while (latch.getCount() > 0) {
                                    execute(statement);
                                    latch.countDown();
                                }
                            } catch (Throwable t) {
                                // ignore VersionConflict.. too many concurrent inserts
                                // retry might not succeed
                                if (!t.getMessage().contains("version conflict")) {
                                    lastThrowable.set(t);
                                }
                            }
                        }
                    }
                );
            }
        }

        latch.await();
        Throwable throwable = lastThrowable.get();
        if (throwable != null) {
            throw throwable;
        }
    }
}
