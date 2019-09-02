/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.test.integration;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.crate.test.integration.ClusterServices.createClusterService;

public class CrateDummyClusterServiceUnitTest extends CrateUnitTest {


    private static final Set<Setting<?>> EMPTY_CLUSTER_SETTINGS = ImmutableSet.of();

    protected static ThreadPool THREAD_POOL;
    protected ClusterService clusterService;

    @BeforeClass
    public static void setupThreadPool() {
        THREAD_POOL = new TestThreadPool(Thread.currentThread().getName());
    }

    @AfterClass
    public static void shutdownThreadPool() {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
    }

    @Before
    public void setupDummyClusterService() {
        clusterService = createClusterService(additionalClusterSettings(), getClass().getSimpleName(), THREAD_POOL);
    }

    @After
    public void cleanup() {
        clusterService.close();
    }

    /**
     * Reset the current cluster service (state) to be able to re-build existing tables.
     */
    public void resetClusterService() {
        cleanup();
        setupDummyClusterService();
    }

    /**
     * Override this method to provide additional cluster settings.
     */
    protected Collection<Setting<?>> additionalClusterSettings() {
        return EMPTY_CLUSTER_SETTINGS;
    }


}
