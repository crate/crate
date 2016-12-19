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
import io.crate.monitor.ThreadPools;
import io.crate.monitor.ZeroExtendedNodeInfo;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.DummyOsInfo;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class NodeStatsContextTest extends CrateUnitTest {

    private ExtendedNodeInfo extendedNodeInfo;
    private ThreadPool threadPool;

    @After
    public void shutdown() throws Exception {
        threadPool.shutdown();
        threadPool.awaitTermination(1000L, TimeUnit.MILLISECONDS);
    }

    @Before
    public void prepare() throws Exception {
        extendedNodeInfo = new ZeroExtendedNodeInfo();
        threadPool = new TestThreadPool("dummy");
    }

    @Test
    public void testStreamContext() throws Exception {
        NodeStatsContext ctx1 = new NodeStatsContext(true);
        ctx1.id(BytesRefs.toBytesRef("93c7ff92-52fa-11e6-aad8-3c15c2d3ad18"));
        ctx1.name(BytesRefs.toBytesRef("crate1"));
        ctx1.hostname(BytesRefs.toBytesRef("crate1.example.com"));
        ctx1.version(Version.CURRENT);
        ctx1.build(Build.CURRENT);
        ctx1.restUrl(BytesRefs.toBytesRef("10.0.0.1:4200"));
        ctx1.port(new HashMap<String, Integer>(2) {{
            put("http", 4200);
            put("transport", 4300);
        }});
        ctx1.jvmStats(JvmStats.jvmStats());
        ctx1.osInfo(DummyOsInfo.INSTANCE);
        ProcessProbe processProbe = ProcessProbe.getInstance();
        ctx1.processStats(processProbe.processStats());
        OsProbe osProbe = OsProbe.getInstance();
        ctx1.osStats(osProbe.osStats());
        ctx1.extendedOsStats(extendedNodeInfo.osStats());
        ctx1.networkStats(extendedNodeInfo.networkStats());
        ctx1.extendedProcessCpuStats(extendedNodeInfo.processCpuStats());
        ctx1.extendedFsStats(extendedNodeInfo.fsStats());
        ctx1.threadPools(ThreadPools.newInstance(threadPool));

        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        StreamOutput out = new OutputStreamStreamOutput(outBuffer);
        ctx1.writeTo(out);

        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        NodeStatsContext ctx2 = new NodeStatsContext(true);
        ctx2.readFrom(in);

        assertEquals(ctx1.id(), ctx2.id());
        assertEquals(ctx1.name(), ctx2.name());
        assertEquals(ctx1.hostname(), ctx2.hostname());
        assertEquals(ctx1.version(), ctx2.version());
        assertEquals(ctx1.build().hash(), ctx2.build().hash());
        assertEquals(ctx1.restUrl(), ctx2.restUrl());
        assertEquals(ctx1.port(), ctx2.port());
        assertEquals(ctx1.jvmStats().getTimestamp(), ctx2.jvmStats().getTimestamp());
        assertEquals(ctx1.osInfo().getArch(), ctx2.osInfo().getArch());
        assertEquals(ctx1.processStats().getTimestamp(), ctx2.processStats().getTimestamp());
        assertEquals(ctx1.osStats().getTimestamp(), ctx2.osStats().getTimestamp());
        assertEquals(ctx1.extendedOsStats().uptime(), ctx2.extendedOsStats().uptime());
        assertEquals(ctx1.networkStats().timestamp(), ctx2.networkStats().timestamp());
        assertEquals(ctx1.extendedProcessCpuStats().percent(), ctx2.extendedProcessCpuStats().percent());
        assertEquals(ctx1.extendedFsStats().size(), ctx2.extendedFsStats().size());
        assertEquals(ctx1.threadPools(), ctx2.threadPools());
    }

    @Test
    public void testStreamEmptyContext() throws Exception {
        NodeStatsContext ctx1 = new NodeStatsContext(false);
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        StreamOutput out = new OutputStreamStreamOutput(outBuffer);
        ctx1.writeTo(out);

        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        NodeStatsContext ctx2 = new NodeStatsContext(false);
        ctx2.readFrom(in);

        assertNull(ctx2.id());
        assertNull(ctx2.name());
        assertNull(ctx2.hostname());
        assertNull(ctx2.restUrl());
        assertNull(ctx2.port());
        assertNull(ctx2.jvmStats());
        assertNull(ctx2.osInfo());
        assertNull(ctx2.processStats());
        assertNull(ctx2.osStats());
        assertNull(ctx2.extendedOsStats());
        assertNull(ctx2.networkStats());
        assertNull(ctx2.extendedProcessCpuStats());
        assertNull(ctx2.extendedFsStats());
        assertNull(ctx2.threadPools());
    }

    @Test
    public void testStreamThreadPools() throws Exception {
        ThreadPools pools1 = ThreadPools.newInstance();
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
        ThreadPools pools2 = ThreadPools.newInstance();
        pools2.readFrom(in);

        assertEquals(pools1, pools2);
    }

    @Test
    public void testStreamThreadPoolsContext() throws Exception {
        ThreadPools.ThreadPoolExecutorContext ctx1 = new ThreadPools.ThreadPoolExecutorContext(
            10, 15, 20, 50, 1_000_000_000_000_000L, 1L);

        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        StreamOutput out = new OutputStreamStreamOutput(outBuffer);
        ctx1.writeTo(out);

        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        ThreadPools.ThreadPoolExecutorContext ctx2 = new ThreadPools.ThreadPoolExecutorContext();
        ctx2.readFrom(in);

        assertEquals(ctx1, ctx2);
    }

    @Test
    public void testStreamContextWithNullPorts() throws Exception {
        NodeStatsContext ctx1 = new NodeStatsContext(false);
        ctx1.port(new HashMap<String, Integer>() {{
            put("http", null);
            put("transport", 4300);
        }});
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        StreamOutput out = new OutputStreamStreamOutput(outBuffer);
        ctx1.writeTo(out);

        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        NodeStatsContext ctx2 = new NodeStatsContext(false);
        ctx2.readFrom(in);

        assertThat(ctx2.port().get("http"), nullValue());
        assertThat(ctx2.port().get("transport"), is(4300));
    }
}
