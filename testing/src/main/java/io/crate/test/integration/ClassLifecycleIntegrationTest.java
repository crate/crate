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
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Random;

/**
 * This base class provides a testCluster that will keep its state across all test methods and will
 * only reset itself {@link #afterClass()}
 *
 * This will be a lot faster then having to wipe the state after each test method.
 * The downside is that the tests aren't as isolated and if data is created it might affect other tests.
 *
 * For all other Integration test purposes use {@link io.crate.test.integration.CrateIntegrationTest}
 */
@ThreadLeakScope(value = ThreadLeakScope.Scope.NONE)
public class ClassLifecycleIntegrationTest extends AbstractRandomizedTest {


    static {
        ESLoggerFactory.getRootLogger().setLevel("WARN");
        Loggers.getLogger("org.elasticsearch.http").setLevel("INFO");
    }


    public static CrateTestCluster GLOBAL_CLUSTER;
    private static Random random;

    @BeforeClass
    public synchronized static void beforeClass() {
        long CLUSTER_SEED = System.nanoTime();
        if (random == null) {
            random = new Random(CLUSTER_SEED);
        }
        if (GLOBAL_CLUSTER == null) {
            GLOBAL_CLUSTER = new CrateTestCluster(
                    CLUSTER_SEED,
                    CrateTestCluster.clusterName("shared", ElasticsearchTestCase.CHILD_VM_ID, CLUSTER_SEED)
            );
        }
        GLOBAL_CLUSTER.beforeTest(random);
    }

    @AfterClass
    public synchronized static void afterClass() throws Exception {
        if (GLOBAL_CLUSTER != null) {
            GLOBAL_CLUSTER.client().admin().indices().prepareDelete("_all").execute().get();
            GLOBAL_CLUSTER.afterTest();
            GLOBAL_CLUSTER = null;
        }

        if (random != null) {
            random = null;
        }
    }
}