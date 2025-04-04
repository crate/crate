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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.DummyOsInfo;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.monitor.ExtendedNodeInfo;
import io.crate.protocols.ConnectionStats;

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
        var ctx1 = generateNodeStatsContext();

        var outBuffer = new ByteArrayOutputStream();
        var out = new OutputStreamStreamOutput(outBuffer);
        ctx1.writeTo(out);

        var inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        var in = new InputStreamStreamInput(inBuffer);
        var ctx2 = new NodeStatsContext(in, true);

        assertThat(ctx2.id()).isEqualTo(ctx1.id());
        assertThat(ctx2.name()).isEqualTo(ctx1.name());
        assertThat(ctx2.hostname()).isEqualTo(ctx1.hostname());
        assertThat(ctx2.timestamp()).isEqualTo(100L);
        assertThat(ctx2.version()).isEqualTo(ctx1.version());
        assertThat(ctx2.build().hash()).isEqualTo(ctx1.build().hash());
        assertThat(ctx2.restUrl()).isEqualTo(ctx1.restUrl());
        assertThat(ctx2.attributes()).isEqualTo(ctx1.attributes());
        assertThat(ctx2.pgPort()).isEqualTo(ctx1.pgPort());
        assertThat(ctx2.httpPort()).isEqualTo(ctx1.httpPort());
        assertThat(ctx2.transportPort()).isEqualTo(ctx1.transportPort());
        assertThat(ctx2.jvmStats().getTimestamp()).isEqualTo(ctx1.jvmStats().getTimestamp());
        assertThat(ctx2.osInfo().getArch()).isEqualTo(ctx1.osInfo().getArch());
        assertThat(ctx2.processStats().getTimestamp()).isEqualTo(ctx1.processStats().getTimestamp());
        assertThat(ctx2.osStats().getTimestamp()).isEqualTo(ctx1.osStats().getTimestamp());
        assertThat(ctx2.fsInfo().getIoStats().getTotalOperations()).isEqualTo(ctx1.fsInfo().getIoStats().getTotalOperations());
        assertThat(ctx2.extendedOsStats().uptime()).isEqualTo(ctx1.extendedOsStats().uptime());
        assertThat(ctx2.threadPools().getFirst().active()).isEqualTo(ctx1.threadPools().getFirst().active());
        assertThat(ctx2.httpStats().total()).isEqualTo(ctx1.httpStats().total());
        assertThat(ctx2.psqlStats().receivedBytes()).isEqualTo(ctx1.psqlStats().receivedBytes());
        assertThat(ctx2.transportStats().sentMsgs()).isEqualTo(ctx1.transportStats().sentMsgs());
        assertThat(ctx2.clusterStateVersion()).isEqualTo(ctx1.clusterStateVersion());
        assertThat(ctx2.osName()).isEqualTo(ctx1.osName());
        assertThat(ctx2.osArch()).isEqualTo(ctx1.osArch());
        assertThat(ctx2.osVersion()).isEqualTo(ctx1.osVersion());
        assertThat(ctx2.javaVersion()).isEqualTo(ctx1.javaVersion());
        assertThat(ctx2.jvmVersion()).isEqualTo(ctx1.jvmVersion());
        assertThat(ctx2.jvmName()).isEqualTo(ctx1.jvmName());
        assertThat(ctx2.jvmVendor()).isEqualTo(ctx1.jvmVendor());
    }

    @Test
    public void testStreamEmptyContext() throws Exception {
        NodeStatsContext ctx1 = new NodeStatsContext(false);
        var out = new BytesStreamOutput();
        ctx1.writeTo(out);

        var in = out.bytes().streamInput();
        NodeStatsContext ctx2 = new NodeStatsContext(in, false);

        assertThat(ctx2.id()).isNull();
        assertThat(ctx2.name()).isNull();
        assertThat(ctx2.hostname()).isNull();
        assertThat(ctx2.timestamp()).isZero();
        assertThat(ctx2.version()).isNull();
        assertThat(ctx2.build()).isNull();
        assertThat(ctx2.restUrl()).isNull();
        assertThat(ctx2.attributes()).isNull();
        assertThat(ctx2.pgPort()).isNull();
        assertThat(ctx2.httpPort()).isNull();
        assertThat(ctx2.transportPort()).isNull();
        assertThat(ctx2.jvmStats()).isNull();
        assertThat(ctx2.osInfo()).isNull();
        assertThat(ctx2.processStats()).isNull();
        assertThat(ctx2.osStats()).isNull();
        assertThat(ctx2.fsInfo()).isNull();
        assertThat(ctx2.extendedOsStats()).isNull();
        assertThat(ctx2.threadPools()).isNull();
        assertThat(ctx2.httpStats()).isNull();
        assertThat(ctx2.psqlStats()).isNull();
        assertThat(ctx2.transportStats()).isNull();
        assertThat(ctx2.clusterStateVersion()).isZero();
        assertThat(ctx2.osName()).isNull();
        assertThat(ctx2.osArch()).isNull();
        assertThat(ctx2.osVersion()).isNull();
        assertThat(ctx2.javaVersion()).isNull();
        assertThat(ctx2.jvmVersion()).isNull();
        assertThat(ctx2.jvmName()).isNull();
        assertThat(ctx2.jvmVendor()).isNull();
    }

    @Test
    public void test_streaming_network_stats_bwc() throws Exception {
        var ctx1 = generateNodeStatsContext();
        var outBuffer = new ByteArrayOutputStream();
        var out = new OutputStreamStreamOutput(outBuffer);
        out.setVersion(Version.V_5_7_0);
        ctx1.writeTo(out);

        var inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        var in = new InputStreamStreamInput(inBuffer);
        in.setVersion(Version.V_5_7_0);
        var ctx2 = new NodeStatsContext(in, true);
        assertThat(ctx2.httpStats().open()).isEqualTo(ctx1.httpStats().open());
        assertThat(ctx2.httpStats().total()).isEqualTo(ctx1.httpStats().total());
        assertThat(ctx2.httpStats().receivedBytes()).isEqualTo(-1);
        assertThat(ctx2.httpStats().receivedMsgs()).isEqualTo(-1);
        assertThat(ctx2.httpStats().sentBytes()).isEqualTo(-1);
        assertThat(ctx2.httpStats().sentMsgs()).isEqualTo(-1);

        assertThat(ctx2.psqlStats().open()).isEqualTo(ctx1.psqlStats().open());
        assertThat(ctx2.psqlStats().total()).isEqualTo(ctx1.psqlStats().total());
        assertThat(ctx2.psqlStats().receivedBytes()).isEqualTo(-1);
        assertThat(ctx2.psqlStats().receivedMsgs()).isEqualTo(-1);
        assertThat(ctx2.psqlStats().sentBytes()).isEqualTo(-1);
        assertThat(ctx2.psqlStats().sentMsgs()).isEqualTo(-1);

        assertThat(ctx2.transportStats().open()).isEqualTo(ctx1.transportStats().open());
        assertThat(ctx2.transportStats().total()).isEqualTo(-1);
        assertThat(ctx2.transportStats().receivedBytes()).isEqualTo(-1);
        assertThat(ctx2.transportStats().receivedMsgs()).isEqualTo(-1);
        assertThat(ctx2.transportStats().sentBytes()).isEqualTo(-1);
        assertThat(ctx2.transportStats().sentMsgs()).isEqualTo(-1);
    }

    private NodeStatsContext generateNodeStatsContext() {
        var ctx = new NodeStatsContext(true); // sets os/java/jvm automatically
        ctx.id("93c7ff92-52fa-11e6-aad8-3c15c2d3ad18");
        ctx.name("crate1");
        ctx.hostname("crate1.example.com");
        ctx.timestamp(100L);
        ctx.version(Version.CURRENT);
        ctx.build(Build.CURRENT);
        ctx.restUrl("10.0.0.1:4200");
        ctx.attributes(Map.of("k1", "v1", "k2", "v2"));
        ctx.pgPort(5432);
        ctx.httpPort(4200);
        ctx.transportPort(4300);
        ctx.jvmStats(JvmStats.jvmStats());
        ctx.osInfo(DummyOsInfo.INSTANCE);
        ProcessProbe processProbe = ProcessProbe.getInstance();
        ctx.processStats(processProbe.processStats());
        OsProbe osProbe = OsProbe.getInstance();
        ctx.osStats(osProbe.osStats());
        ctx.fsInfo(new FsInfo(
            123456789,
            new FsInfo.IoStats(new FsInfo.DeviceStats[]{ new FsInfo.DeviceStats(1,2,"foo", 3, 4, 5, 6, null)}),
            new FsInfo.Path[] {new FsInfo.Path("foo", "bar", 1, 2, 3)}));
        ctx.extendedOsStats(extendedNodeInfo.osStats());
        ctx.threadPools(threadPool.stats());
        ctx.httpStats(new ConnectionStats(1, 2, 3, 4, 5, 6));
        ctx.psqlStats(new ConnectionStats(11, 22, 33, 44, 55, 66));
        ctx.transportStats(new ConnectionStats(111, 222, 333, 444, 555, 666));
        ctx.clusterStateVersion(10L);

        return ctx;
    }
}
