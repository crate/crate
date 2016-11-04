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

package io.crate.monitor;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ExtendedStatsTest {

    private BytesStreamOutput out;

    @Before
    public void beforeTest() {
        out = new BytesStreamOutput();
    }

    @Test
    public void testExtendedOsStatsSerialization() throws IOException {
        ExtendedOsStats statOut = new ExtendedOsStats();
        statOut.loadAverage(new double[]{1.1, 2.3});
        statOut.timestamp(1L);
        statOut.uptime(2L);
        statOut.cpu(new ExtendedOsStats.Cpu((short) 1, (short) 2, (short) 3, (short) 4));
        statOut.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ExtendedOsStats statIn = ExtendedOsStats.readExtendedOsStat(in);

        assertThat(statOut.uptime(), is(statIn.uptime()));
        assertThat(statOut.timestamp(), is(statIn.timestamp()));
        assertThat(Arrays.equals(statIn.loadAverage(), statIn.loadAverage()), is(true));
        assertThat(statOut.cpu().idle(), is(statIn.cpu().idle()));
        assertThat(statOut.cpu().stolen(), is(statIn.cpu().stolen()));
        assertThat(statOut.cpu().sys(), is(statIn.cpu().sys()));
        assertThat(statOut.cpu().user(), is(statIn.cpu().user()));
    }

    @Test
    public void testExtendedOsStatsSerializationDefault() throws IOException {
        ExtendedOsStats statOut = new ExtendedOsStats();
        statOut.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        ExtendedOsStats statIn = ExtendedOsStats.readExtendedOsStat(in);
        assertThat(statOut.uptime(), is(statIn.uptime()));
    }

    @Test
    public void testExtendedNetworkStatsSerialization() throws IOException {
        ExtendedNetworkStats statOut = new ExtendedNetworkStats();
        statOut.timestamp(1L);
        statOut.tcp(new ExtendedNetworkStats.Tcp(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L));
        statOut.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ExtendedNetworkStats statIn = ExtendedNetworkStats.readExtendedNetworkStats(in);
        assertThat(statOut.timestamp(), is(statIn.timestamp()));
        assertThat(statOut.tcp().inErrs(), is(statIn.tcp().inErrs()));
        assertThat(statOut.tcp().estabResets(), is(statIn.tcp().estabResets()));
        assertThat(statOut.tcp().retransSegs(), is(statIn.tcp().retransSegs()));
    }

    @Test
    public void testExtendedNetworkStatsSerializationDefault() throws IOException {
        ExtendedNetworkStats statOut = new ExtendedNetworkStats();
        statOut.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        ExtendedNetworkStats statIn = ExtendedNetworkStats.readExtendedNetworkStats(in);
        assertThat(statOut.timestamp(), is(statIn.timestamp()));
    }

    @Test
    public void testExtendedProcessCpuStatsSerialization() throws IOException {
        ExtendedProcessCpuStats statOut = new ExtendedProcessCpuStats();
        statOut.sys(TimeValue.timeValueMillis(1));
        statOut.total(TimeValue.timeValueMillis(2));
        statOut.user(TimeValue.timeValueMillis(3));
        statOut.percent((short) 4);
        statOut.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ExtendedProcessCpuStats statIn = ExtendedProcessCpuStats.readExtendedProcessCpuStats(in);
        assertThat(statOut.sys(), is(statIn.sys()));
        assertThat(statOut.percent(), is(statIn.percent()));
        assertThat(statOut.total(), is(statIn.total()));
        assertThat(statOut.user(), is(statIn.user()));
    }

    @Test
    public void testExtendedProcessCpuStatsSerializationDefault() throws IOException {
        ExtendedProcessCpuStats statOut = new ExtendedProcessCpuStats();
        statOut.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ExtendedProcessCpuStats statIn = ExtendedProcessCpuStats.readExtendedProcessCpuStats(in);
        assertThat(statOut.sys(), is(statIn.sys()));
    }

    @Test
    public void testExtendedFsStatsSerialization() throws IOException {
        ExtendedFsStats statOut = new ExtendedFsStats();
        statOut.infos(new ExtendedFsStats.Info[]{new ExtendedFsStats.Info("test_infos", null, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L)});
        statOut.total(new ExtendedFsStats.Info("test_total", null, 10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L));
        statOut.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ExtendedFsStats statIn = ExtendedFsStats.readExtendedFsStats(in);
        assertThat(statOut.total().dev(), is(statIn.total().dev()));
        assertThat(statOut.infos().length, is(1));
        assertThat(statOut.infos()[0].diskReads(), is(statIn.infos()[0].diskReads()));
    }

    @Test
    public void testExtendedFsStatsSerializationDefault() throws IOException {
        ExtendedFsStats statOut = new ExtendedFsStats();
        statOut.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ExtendedFsStats statIn = ExtendedFsStats.readExtendedFsStats(in);
        assertThat(statOut.total().dev(), is(statIn.total().dev()));
    }

}
