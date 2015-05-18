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
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.test.integration;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.apache.lucene.util.AbstractRandomizedTest;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.SettingsSource;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Random;

import static org.elasticsearch.test.ElasticsearchTestCase.assertBusy;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;

/**
 * This base class provides a testCluster that will keep its state across all test methods and will
 * only reset itself {@link #afterClass()}
 *
 * This will be a lot faster then having to wipe the state after each test method.
 * The downside is that the tests aren't as isolated and if data is created it might affect other tests.
 *
 */
@ThreadLeakScope(value = ThreadLeakScope.Scope.NONE)
public class ClassLifecycleIntegrationTest extends AbstractRandomizedTest {

    public static InternalTestCluster GLOBAL_CLUSTER;
    private static Random random;

    public static final SettingsSource SETTINGS_SOURCE = new SettingsSource() {
        @Override
        public Settings node(int nodeOrdinal) {
            return ImmutableSettings.builder()
                    .put(InternalNode.HTTP_ENABLED, false)
                    .put("plugin.types", "io.crate.plugin.CrateCorePlugin").build();
        }

        @Override
        public Settings transportClient() {
            return ImmutableSettings.EMPTY;
        }
    };

    @BeforeClass
    public synchronized static void beforeClass() throws IOException {
        long CLUSTER_SEED = System.nanoTime();
        if (random == null) {
            random = new Random(CLUSTER_SEED);
        }
        if (GLOBAL_CLUSTER == null) {
            GLOBAL_CLUSTER = new InternalTestCluster(
                    CLUSTER_SEED,
                    2,
                    2,
                    InternalTestCluster.clusterName("shared", Integer.toString(CHILD_JVM_ID), CLUSTER_SEED),
                    SETTINGS_SOURCE,
                    0,
                    false,
                    CHILD_JVM_ID,
                    "shared"
            );
        }
        GLOBAL_CLUSTER.beforeTest(random, 0.0D);
    }

    @AfterClass
    public synchronized static void afterClass() throws Exception {
        if (GLOBAL_CLUSTER != null) {
            GLOBAL_CLUSTER.client().admin().indices().prepareDelete("_all").execute().get();
            GLOBAL_CLUSTER.afterTest();
            GLOBAL_CLUSTER.close();
            GLOBAL_CLUSTER = null;
        }

        if (random != null) {
            random = null;
        }
    }

    /**
     * Waits until all nodes have no pending tasks.
     */
    public void waitNoPendingTasksOnAll() throws Exception {
        assertNoTimeout(GLOBAL_CLUSTER.client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get());
        assertBusy(new Runnable() {
            @Override
            public void run() {
                PendingClusterTasksResponse pendingTasks = GLOBAL_CLUSTER.client().admin().cluster().preparePendingClusterTasks().setLocal(true).get();
                assertThat("client " + GLOBAL_CLUSTER.client()  + " still has pending tasks " +
                        pendingTasks.prettyPrint(), pendingTasks, Matchers.emptyIterable());
            }
        });
        assertNoTimeout(GLOBAL_CLUSTER.client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get());
    }
}
