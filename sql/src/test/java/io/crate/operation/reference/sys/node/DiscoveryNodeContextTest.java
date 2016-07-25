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

package io.crate.operation.reference.sys.node;

import io.crate.Build;
import io.crate.Version;
import io.crate.monitor.ExtendedNodeInfo;
import io.crate.monitor.MonitorModule;
import io.crate.monitor.ThreadPools;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.concurrent.XRejectedExecutionHandler;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeEnvironmentModule;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.DummyOsInfo;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DiscoveryNodeContextTest extends CrateUnitTest {

    private ExtendedNodeInfo extendedNodeInfo;
    private ThreadPool threadPool;

    @After
    public void shutdown() throws Exception {
        threadPool.shutdown();
        threadPool.awaitTermination(1000L, TimeUnit.MILLISECONDS);
    }

    @Before
    public void prepare() throws Exception {
        Settings settings = Settings.builder()
            .put("name", getClass().getName())
            .build();

        NodeEnvironment nodeEnvironment = mock(NodeEnvironment.class);
        when(nodeEnvironment.hasNodeFile()).thenReturn(true);
        Path tempDir = createTempDir();
        NodeEnvironment.NodePath[] dataLocations = new NodeEnvironment.NodePath[]{new NodeEnvironment.NodePath(tempDir, mock(Environment.class))};
        when(nodeEnvironment.nodePaths()).thenReturn(dataLocations);

        NodeEnvironmentModule nodeEnvironmentModule = new NodeEnvironmentModule(nodeEnvironment);
        MonitorModule monitorModule = new MonitorModule(settings);
        Injector injector = new ModulesBuilder().add(
            new ThreadPoolModule(new ThreadPool(settings)),
            new SettingsModule(settings),
            monitorModule,
            nodeEnvironmentModule
        ).createInjector();
        extendedNodeInfo = injector.getInstance(ExtendedNodeInfo.class);
        threadPool = injector.getInstance(ThreadPool.class);
    }

    @Test
    public void testStreamContext() throws Exception {
        DiscoveryNodeContext ctx1 = new DiscoveryNodeContext(false);
        ctx1.id = BytesRefs.toBytesRef("93c7ff92-52fa-11e6-aad8-3c15c2d3ad18");
        ctx1.name = BytesRefs.toBytesRef("crate1");
        ctx1.hostname = BytesRefs.toBytesRef("crate1.example.com");
        ctx1.version = Version.CURRENT;
        ctx1.build = Build.CURRENT;
        ctx1.restUrl = BytesRefs.toBytesRef("10.0.0.1:4200");
        ctx1.port = new HashMap<String, Integer>(2) {{
            put("http", 4200);
            put("transport", 4300);
        }};
        ctx1.jvmStats = JvmStats.jvmStats();
        ctx1.osInfo = DummyOsInfo.INSTANCE;
        ProcessProbe processProbe = ProcessProbe.getInstance();
        ctx1.processStats = processProbe.processStats();
        OsProbe osProbe = OsProbe.getInstance();
        ctx1.osStats = osProbe.osStats();
        ctx1.extendedOsStats = extendedNodeInfo.osStats();
        ctx1.networkStats = extendedNodeInfo.networkStats();
        ctx1.extendedProcessCpuStats= extendedNodeInfo.processCpuStats();
        ctx1.extendedFsStats= extendedNodeInfo.fsStats();
        ctx1.threadPools = threadPoolInfo();

        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        StreamOutput out = new OutputStreamStreamOutput(outBuffer);
        ctx1.writeTo(out);

        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        DiscoveryNodeContext ctx2 = new DiscoveryNodeContext(false);
        ctx2.readFrom(in);

        assertEquals(ctx1, ctx2);
    }

    private ThreadPools threadPoolInfo() {
        ThreadPools pools = new ThreadPools();
        for (ThreadPool.Info info : threadPool.info()) {
            ThreadPoolExecutor executor = (ThreadPoolExecutor) threadPool.executor(info.getName());
            long rejected = -1;
            RejectedExecutionHandler rejectedExecutionHandler = executor.getRejectedExecutionHandler();
            if (rejectedExecutionHandler instanceof XRejectedExecutionHandler) {
                rejected = ((XRejectedExecutionHandler) rejectedExecutionHandler).rejected();
            }
            ThreadPools.ThreadPoolExecutorContext ctx = new ThreadPools.ThreadPoolExecutorContext(
                executor.getQueue().size(),
                executor.getActiveCount(),
                executor.getLargestPoolSize(),
                executor.getPoolSize(),
                executor.getCompletedTaskCount(),
                rejected
            );
            pools.add(info.getName(), ctx);
        }
        return pools;
    }

    @Test
    public void testStreamEmptyContext() throws Exception {
        DiscoveryNodeContext ctx1 = new DiscoveryNodeContext(true);
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        StreamOutput out = new OutputStreamStreamOutput(outBuffer);
        ctx1.writeTo(out);

        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        DiscoveryNodeContext ctx2 = new DiscoveryNodeContext(true);
        ctx2.readFrom(in);

        assertNull(ctx2.id);
        assertNull(ctx2.name);
        assertNull(ctx2.hostname);
        assertNull(ctx2.restUrl);
        assertNull(ctx2.port);
        assertNull(ctx2.jvmStats);
        assertNull(ctx2.osInfo);
        assertNull(ctx2.processStats);
        assertNull(ctx2.osStats);
        assertNull(ctx2.extendedOsStats);
        assertNull(ctx2.networkStats);
        assertNull(ctx2.extendedProcessCpuStats);
        assertNull(ctx2.extendedFsStats);
        assertNull(ctx2.threadPools);
    }

    @Test
    public void testStreamThreadPools() throws Exception {
        ThreadPools pools1 = new ThreadPools();
        int size = 3;
        for (int i = 0; i < size; i++) {
            ThreadPools.ThreadPoolExecutorContext ctx = new ThreadPools.ThreadPoolExecutorContext(
                10 * i + 1,
                10 * i + 2,
                10 * i + 3,
                10 * i + 4,
                100L * i + 1L,
                100L * i + 2L);
            pools1.add(String.format("threadpool-%d", i), ctx);
        }

        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        StreamOutput out = new OutputStreamStreamOutput(outBuffer);
        pools1.writeTo(out);

        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        ThreadPools pools2 = new ThreadPools();
        pools2.readFrom(in);

        assertEquals(pools1, pools2);
    }

    @Test
    public void testStreamThreadPoolsContext() throws Exception {
        ThreadPools.ThreadPoolExecutorContext ctx1 = new ThreadPools.ThreadPoolExecutorContext(
            10, null, 20, 50, 1_000_000_000_000_000L, null);

        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        StreamOutput out = new OutputStreamStreamOutput(outBuffer);
        ctx1.writeTo(out);

        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        ThreadPools.ThreadPoolExecutorContext ctx2 = new ThreadPools.ThreadPoolExecutorContext();
        ctx2.readFrom(in);

        assertEquals(ctx1, ctx2);
        assertNull(ctx2.activeCount());
        assertNull(ctx2.rejectedCount());

    }
}
