/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.stress;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.concurrent.ThreadedExecutionRule;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractIntegrationStressTest extends SQLTransportIntegrationTest {

    @Rule
    public ThreadedExecutionRule threadedExecutionRule = new ThreadedExecutionRule();


    private final AtomicBoolean firstPrepared = new AtomicBoolean(false);
    private final SettableFuture<Void> preparedFuture = SettableFuture.create();
    private final AtomicInteger stillRunning = new AtomicInteger(0);
    private final SettableFuture<Void> cleanUpFuture = SettableFuture.create();

    /**
     * preparation only executed in the first thread that reaches @Before
     */
    public void prepareFirst() throws Exception {}

    /**
     * cleanup only executed by the last thread that reaches @After
     */
    public void cleanUpLast() throws Exception {}

    @Before
    public void delegateToPrepareFirst() throws Exception {
        if (firstPrepared.compareAndSet(false, true)) {
            prepareFirst();
            preparedFuture.set(null);
        } else {
            preparedFuture.get();
        }
        stillRunning.incrementAndGet();
    }

    @After
    public void waitForAllTestsToReachAfter() throws Exception {
        if (stillRunning.decrementAndGet() > 0) {
            cleanUpFuture.get();
        } else {
            // der letzte macht das licht aus
            cleanUpLast();
            cleanUpFuture.set(null);
        }
    }
}
