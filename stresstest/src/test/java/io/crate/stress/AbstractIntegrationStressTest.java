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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.sql.*;
import io.crate.concurrent.ThreadedExecutionRule;
import io.crate.test.integration.ClassLifecycleIntegrationTest;
import org.apache.lucene.util.AbstractRandomizedTest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractIntegrationStressTest extends RandomizedTest {

    private static final String SQL_REQUEST_TIMEOUT = "CRATE_TESTS_SQL_REQUEST_TIMEOUT";
    private static final TimeValue REQUEST_TIMEOUT = new TimeValue(Long.parseLong(
            MoreObjects.firstNonNull(System.getenv(SQL_REQUEST_TIMEOUT), "5")), TimeUnit.SECONDS);

    @Rule
    public ThreadedExecutionRule threadedExecutionRule = new ThreadedExecutionRule();

    private static final long SEED = System.nanoTime();
    protected static String NODE1;
    protected static String NODE2;
    protected static InternalTestCluster CLUSTER;

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

    @BeforeClass
    public synchronized static void beforeClass() throws IOException {
        if (CLUSTER == null) {
            CLUSTER = new InternalTestCluster(
                    SEED,
                    0,
                    0,
                    InternalTestCluster.clusterName("shared", Integer.toString(AbstractRandomizedTest.CHILD_JVM_ID), SEED),
                    ClassLifecycleIntegrationTest.SETTINGS_SOURCE,
                    0,
                    false,
                    AbstractRandomizedTest.CHILD_JVM_ID,
                    "shared"
            );
        }
        CLUSTER.beforeTest(getRandom(), 0.0d);
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        try {
            CLUSTER.client().admin().indices().prepareDelete("_all").execute().actionGet();
        } catch (IndexMissingException e) {
            // fine
        }
        CLUSTER.afterTest();
        CLUSTER.close();
        CLUSTER = null;
        NODE1 = null;
        NODE2 = null;
    }

    @Before
    public void delegateToPrepareFirst() throws Exception {
        setupNodes();
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

    /**
     * @return a random available port for binding
     */
    protected int randomAvailablePort()  {
        int port;
        try {
            ServerSocket socket = new ServerSocket(0);
            port = socket.getLocalPort();
            socket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return port;
    }

    protected Settings nodeSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder()
                .put("plugin.types", "io.crate.plugin.CrateCorePlugin")
                .put("transport.tcp.port", randomAvailablePort())
                .put("index.store.type", "memory");
        return builder.build();
    }

    private void setupNodes() throws Exception {
        if (NODE1 == null) {
            NODE1 = CLUSTER.startNode(nodeSettings());
        }
        if (NODE2 == null) {
            NODE2 = CLUSTER.startNode(nodeSettings());
        }
    }

    public SQLResponse execute(String stmt, Object[] args) {
        return execute(new SQLRequest(stmt, args), REQUEST_TIMEOUT);
    }

    public SQLResponse execute(String stmt, Object[] args, TimeValue timeout) {
        return execute(new SQLRequest(stmt, args), timeout);
    }

    public SQLResponse execute(String stmt) {
        return execute(stmt, SQLRequest.EMPTY_ARGS);
    }

    public SQLResponse execute(String stmt, TimeValue timeout) {
        return execute(stmt, SQLRequest.EMPTY_ARGS, timeout);
    }

    public SQLResponse execute(SQLRequest request, TimeValue timeout) {
        return CLUSTER.client().execute(SQLAction.INSTANCE, request).actionGet(timeout);
    }

    public SQLBulkResponse execute(String stmt, Object[][] bulkArgs) {
        return execute(stmt, bulkArgs, REQUEST_TIMEOUT);
    }

    public SQLBulkResponse execute(String stmt, Object[][] bulkArgs, TimeValue timeout) {
        return CLUSTER.client().execute(SQLBulkAction.INSTANCE, new SQLBulkRequest(stmt, bulkArgs)).actionGet(timeout);
    }

    public void ensureGreen() {
        CLUSTER.client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet(REQUEST_TIMEOUT);
    }

    public void ensureYellow() {
        CLUSTER.client().admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet(REQUEST_TIMEOUT);
    }
}
