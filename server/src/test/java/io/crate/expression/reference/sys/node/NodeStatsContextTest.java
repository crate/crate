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

package io.crate.expression.reference.sys.node;

import io.crate.monitor.ExtendedNodeInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
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
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class NodeStatsContextTest extends ESTestCase {

    private ExtendedNodeInfo extendedNodeInfo;
    private ThreadPool threadPool;

    @After
    public void shutdown() throws Exception {
        threadPool.shutdown();
        threadPool.awaitTermination(1000L, TimeUnit.MILLISECONDS);
    }

    @Before
    public void prepare() throws Exception {
        extendedNodeInfo = new ExtendedNodeInfo();
        threadPool = new TestThreadPool("dummy");
    }

    @Test
    public void testStreamContext() throws Exception {
        NodeStatsContext ctx1 = new NodeStatsContext(true);
        ctx1.id("93c7ff92-52fa-11e6-aad8-3c15c2d3ad18");
        ctx1.name("crate1");
        ctx1.hostname("crate1.example.com");
        ctx1.timestamp(100L);
        ctx1.version(Version.CURRENT);
        ctx1.build(Build.CURRENT);
        ctx1.httpPort(4200);
        ctx1.transportPort(4300);
        ctx1.restUrl("10.0.0.1:4200");
        ctx1.jvmStats(JvmStats.jvmStats());
        ctx1.osInfo(DummyOsInfo.INSTANCE);
        ProcessProbe processProbe = ProcessProbe.getInstance();
        ctx1.processStats(processProbe.processStats());
        OsProbe osProbe = OsProbe.getInstance();
        ctx1.osStats(osProbe.osStats());
        ctx1.extendedOsStats(extendedNodeInfo.osStats());
        ctx1.threadPools(threadPool.stats());
        ctx1.clusterStateVersion(10L);

        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        StreamOutput out = new OutputStreamStreamOutput(outBuffer);
        ctx1.writeTo(out);

        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        NodeStatsContext ctx2 = new NodeStatsContext(in, true);

        assertThat(ctx1.id(), is(ctx2.id()));
        assertThat(ctx1.name(), is(ctx2.name()));
        assertThat(ctx1.hostname(), is(ctx2.hostname()));
        assertThat(ctx1.timestamp(), is(100L));
        assertThat(ctx1.version(), is(ctx2.version()));
        assertThat(ctx1.build().hash(), is(ctx2.build().hash()));
        assertThat(ctx1.restUrl(), is(ctx2.restUrl()));
        assertThat(ctx1.httpPort(), is(ctx2.httpPort()));
        assertThat(ctx1.transportPort(), is(ctx2.transportPort()));
        assertThat(ctx1.pgPort(), is(ctx2.pgPort()));
        assertThat(ctx1.jvmStats().getTimestamp(), is(ctx2.jvmStats().getTimestamp()));
        assertThat(ctx1.osInfo().getArch(), is(ctx2.osInfo().getArch()));
        assertThat(ctx1.processStats().getTimestamp(), is(ctx2.processStats().getTimestamp()));
        assertThat(ctx1.osStats().getTimestamp(), is(ctx2.osStats().getTimestamp()));
        assertThat(ctx1.extendedOsStats().uptime(), is(ctx2.extendedOsStats().uptime()));
        assertThat(ctx1.threadPools().iterator().next().getActive(), is(ctx2.threadPools().iterator().next().getActive()));
        assertThat(ctx1.clusterStateVersion(), is(ctx2.clusterStateVersion()));
    }

    @Test
    public void testStreamEmptyContext() throws Exception {
        NodeStatsContext ctx1 = new NodeStatsContext(false);
        var out = new BytesStreamOutput();
        ctx1.writeTo(out);

        var in = out.bytes().streamInput();
        NodeStatsContext ctx2 = new NodeStatsContext(in, false);

        assertNull(ctx2.id());
        assertNull(ctx2.name());
        assertNull(ctx2.hostname());
        assertNull(ctx2.restUrl());
        assertNull(ctx2.httpPort());
        assertNull(ctx2.transportPort());
        assertNull(ctx2.pgPort());
        assertNull(ctx2.jvmStats());
        assertNull(ctx2.osInfo());
        assertNull(ctx2.processStats());
        assertNull(ctx2.osStats());
        assertNull(ctx2.extendedOsStats());
        assertNull(ctx2.threadPools());
    }

    @Test
    public void testStreamContextWithNullPorts() throws Exception {
        NodeStatsContext ctx1 = new NodeStatsContext(false);
        ctx1.transportPort(4300);
        ctx1.httpPort(null);
        var out = new BytesStreamOutput();
        ctx1.writeTo(out);

        var in = out.bytes().streamInput();
        NodeStatsContext ctx2 = new NodeStatsContext(in, false);

        assertThat(ctx2.httpPort(), nullValue());
        assertThat(ctx2.transportPort(), is(4300));
    }
}
