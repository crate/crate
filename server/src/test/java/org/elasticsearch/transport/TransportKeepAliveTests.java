/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.function.BiFunction;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.Test;
import org.mockito.Mockito;

import io.crate.common.collections.Tuple;
import io.crate.common.unit.TimeValue;
import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;

public class TransportKeepAliveTests extends ESTestCase {

    private final ConnectionProfile defaultProfile = ConnectionProfile.buildDefaultConnectionProfile(Settings.EMPTY);
    private byte[] expectedPingMessage;
    private BiFunction<CloseableChannel, byte[], ChannelFuture> pingSender;
    private TransportKeepAlive keepAlive;
    private CapturingThreadPool threadPool;

    private static CloseableChannel fakeChannel() {
        return fakeChannel(false);
    }

    private static CloseableChannel fakeChannel(boolean isServer) {
        return new CloseableChannel(new EmbeddedChannel(), isServer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        super.setUp();
        pingSender = mock(BiFunction.class);
        when(pingSender.apply(Mockito.any(), Mockito.any(byte[].class))).thenReturn(mock(ChannelFuture.class));
        threadPool = new CapturingThreadPool();
        keepAlive = new TransportKeepAlive(threadPool, pingSender);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeByte((byte) 'E');
            out.writeByte((byte) 'S');
            out.writeInt(-1);
            expectedPingMessage = new byte[6];
            BytesRef bytesRef = out.bytes().toBytesRef();
            System.arraycopy(bytesRef.bytes, bytesRef.offset, expectedPingMessage, 0, bytesRef.length);
        } catch (IOException e) {
            throw new AssertionError(e.getMessage(), e); // won't happen
        }
    }

    @Override
    public void tearDown() throws Exception {
        threadPool.shutdown();
        super.tearDown();
    }

    @Test
    public void testRegisterNodeConnectionSchedulesKeepAlive() {
        TimeValue pingInterval = TimeValue.timeValueSeconds(randomLongBetween(1, 60));
        ConnectionProfile connectionProfile = new ConnectionProfile.Builder(defaultProfile)
            .setPingInterval(pingInterval)
            .build();

        assertThat(threadPool.scheduledTasks.size()).isEqualTo(0);

        var channel1 = fakeChannel();
        var channel2 = fakeChannel();
        channel1.markAccessed(threadPool.relativeTimeInMillis());
        channel2.markAccessed(threadPool.relativeTimeInMillis());
        keepAlive.registerNodeConnection(Arrays.asList(channel1, channel2), connectionProfile);

        assertThat(threadPool.scheduledTasks.size()).isEqualTo(1);
        Tuple<TimeValue, Runnable> taskTuple = threadPool.scheduledTasks.poll();
        assertThat(taskTuple.v1()).isEqualTo(pingInterval);
        Runnable keepAliveTask = taskTuple.v2();
        assertThat(threadPool.scheduledTasks.size()).isEqualTo(0);
        keepAliveTask.run();

        verify(pingSender, times(1)).apply(same(channel1), eq(expectedPingMessage));
        verify(pingSender, times(1)).apply(same(channel2), eq(expectedPingMessage));

        // Test that the task has rescheduled itself
        assertThat(threadPool.scheduledTasks.size()).isEqualTo(1);
        Tuple<TimeValue, Runnable> rescheduledTask = threadPool.scheduledTasks.poll();
        assertThat(rescheduledTask.v1()).isEqualTo(pingInterval);
    }

    @Test
    public void testRegisterMultipleKeepAliveIntervals() {
        TimeValue pingInterval1 = TimeValue.timeValueSeconds(randomLongBetween(1, 30));
        ConnectionProfile connectionProfile1 = new ConnectionProfile.Builder(defaultProfile)
            .setPingInterval(pingInterval1)
            .build();

        TimeValue pingInterval2 = TimeValue.timeValueSeconds(randomLongBetween(31, 60));
        ConnectionProfile connectionProfile2 = new ConnectionProfile.Builder(defaultProfile)
            .setPingInterval(pingInterval2)
            .build();

        assertThat(threadPool.scheduledTasks.size()).isEqualTo(0);

        var channel1 = fakeChannel();
        var channel2 = fakeChannel();
        channel1.markAccessed(threadPool.relativeTimeInMillis());
        channel2.markAccessed(threadPool.relativeTimeInMillis());
        keepAlive.registerNodeConnection(Collections.singletonList(channel1), connectionProfile1);
        keepAlive.registerNodeConnection(Collections.singletonList(channel2), connectionProfile2);

        assertThat(threadPool.scheduledTasks.size()).isEqualTo(2);
        Tuple<TimeValue, Runnable> taskTuple1 = threadPool.scheduledTasks.poll();
        Tuple<TimeValue, Runnable> taskTuple2 = threadPool.scheduledTasks.poll();
        assertThat(taskTuple1.v1()).isEqualTo(pingInterval1);
        assertThat(taskTuple2.v1()).isEqualTo(pingInterval2);
        Runnable keepAliveTask1 = taskTuple1.v2();
        Runnable keepAliveTask2 = taskTuple1.v2();

        assertThat(threadPool.scheduledTasks.size()).isEqualTo(0);
        keepAliveTask1.run();
        assertThat(threadPool.scheduledTasks.size()).isEqualTo(1);
        keepAliveTask2.run();
        assertThat(threadPool.scheduledTasks.size()).isEqualTo(2);
    }

    @Test
    public void testClosingChannelUnregistersItFromKeepAlive() {
        TimeValue pingInterval1 = TimeValue.timeValueSeconds(randomLongBetween(1, 30));
        ConnectionProfile connectionProfile = new ConnectionProfile.Builder(defaultProfile)
            .setPingInterval(pingInterval1)
            .build();

        var channel1 = fakeChannel();
        var channel2 = fakeChannel();
        channel1.markAccessed(threadPool.relativeTimeInMillis());
        channel2.markAccessed(threadPool.relativeTimeInMillis());
        keepAlive.registerNodeConnection(Collections.singletonList(channel1), connectionProfile);
        keepAlive.registerNodeConnection(Collections.singletonList(channel2), connectionProfile);

        channel1.close();

        Runnable task = threadPool.scheduledTasks.poll().v2();
        task.run();

        verify(pingSender, times(0)).apply(same(channel1), eq(expectedPingMessage));
        verify(pingSender, times(1)).apply(same(channel2), eq(expectedPingMessage));
    }

    @Test
    public void testKeepAliveResponseIfServer() {
        var channel = fakeChannel(true);
        channel.markAccessed(threadPool.relativeTimeInMillis());

        keepAlive.receiveKeepAlive(channel);

        verify(pingSender, times(1)).apply(same(channel), eq(expectedPingMessage));
    }

    @Test
    public void testNoKeepAliveResponseIfClient() {
        var channel = fakeChannel();
        channel.markAccessed(threadPool.relativeTimeInMillis());

        keepAlive.receiveKeepAlive(channel);

        verify(pingSender, times(0)).apply(same(channel), eq(expectedPingMessage));
    }

    @Test
    public void testOnlySendPingIfWeHaveNotWrittenAndReadSinceLastPing() {
        TimeValue pingInterval = TimeValue.timeValueSeconds(15);
        ConnectionProfile connectionProfile = new ConnectionProfile.Builder(defaultProfile)
            .setPingInterval(pingInterval)
            .build();

        var channel1 = fakeChannel();
        var channel2 = fakeChannel();
        channel1.markAccessed(threadPool.relativeTimeInMillis());
        channel2.markAccessed(threadPool.relativeTimeInMillis());
        keepAlive.registerNodeConnection(Arrays.asList(channel1, channel2), connectionProfile);

        Tuple<TimeValue, Runnable> taskTuple = threadPool.scheduledTasks.poll();
        taskTuple.v2().run();

        channel1.markAccessed(threadPool.relativeTimeInMillis() + (pingInterval.millis() / 2));

        taskTuple = threadPool.scheduledTasks.poll();
        taskTuple.v2().run();

        verify(pingSender, times(1)).apply(same(channel1), eq(expectedPingMessage));
        verify(pingSender, times(2)).apply(same(channel2), eq(expectedPingMessage));
    }

    private class CapturingThreadPool extends TestThreadPool {

        private final Deque<Tuple<TimeValue, Runnable>> scheduledTasks = new ArrayDeque<>();

        private CapturingThreadPool() {
            super(getTestName());
        }

        @Override
        public ScheduledCancellable schedule(Runnable task, TimeValue delay, String executor) {
            scheduledTasks.add(new Tuple<>(delay, task));
            return null;
        }
    }
}
