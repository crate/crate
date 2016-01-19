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
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeConfigurationSource;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;

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
public abstract class ClassLifecycleIntegrationTest extends ESTestCase {

    public static InternalTestCluster GLOBAL_CLUSTER;
    private static Random random;

    public static final NodeConfigurationSource SETTINGS_SOURCE = new NodeConfigurationSource() {
        @Override
        public Collection<Class<? extends Plugin>> nodePlugins() {
            try {
                Class<? extends Plugin> aClass = (Class<? extends Plugin>) Class.forName("io.crate.plugin.CrateCorePlugin");
                return Collections.<Class<? extends Plugin>>singletonList(aClass);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Settings nodeSettings(int i) {
            return Settings.builder().put(Node.HTTP_ENABLED, false).build();
        }

        @Override
        public Settings transportClientSettings() {
            return Settings.EMPTY;
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
                    "local",
                    CLUSTER_SEED,
                    createTempDir(),
                    2,
                    2,
                    InternalTestCluster.clusterName("shared", randomLong()),
                    SETTINGS_SOURCE,
                    0,
                    false,
                    "shared",
                    true
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
