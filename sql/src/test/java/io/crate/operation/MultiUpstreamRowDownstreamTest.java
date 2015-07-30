/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation;

import io.crate.core.collections.Row;
import io.crate.core.collections.Row1;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.*;

public class MultiUpstreamRowDownstreamTest extends CrateUnitTest {

    MultiUpstreamRowDownstream multiUpstreamRowDownstream;

    @Before
    public void before() {
        multiUpstreamRowDownstream = new MultiUpstreamRowDownstream();
    }

    @Test
    public void testWithoutDownstreamHandle() throws Exception {
        multiUpstreamRowDownstream.registerUpstream(null);
        multiUpstreamRowDownstream.registerUpstream(null);

        assertThat(multiUpstreamRowDownstream.upstreams(), contains(nullValue(), nullValue()));
        assertThat(multiUpstreamRowDownstream.pendingUpstreams(), is(2));
        assertThat(multiUpstreamRowDownstream.allUpstreamsFinishedSuccessful(), is(false));
        assertThat(multiUpstreamRowDownstream.upstreamsRunning(), is(true));
        assertThat(multiUpstreamRowDownstream.failure(), nullValue());

        multiUpstreamRowDownstream.finish();

        assertThat(multiUpstreamRowDownstream.setNextRow(new Row1(0)), is(true));

        multiUpstreamRowDownstream.finish();
        assertThat(multiUpstreamRowDownstream.pendingUpstreams(), is(0));
        assertThat(multiUpstreamRowDownstream.allUpstreamsFinishedSuccessful(), is(true));
        assertThat(multiUpstreamRowDownstream.upstreamsRunning(), is(false));
    }

    @Test
    public void testFailureWithoutDownstreamHandle() throws Exception {
        multiUpstreamRowDownstream.registerUpstream(null);
        multiUpstreamRowDownstream.registerUpstream(null);
        multiUpstreamRowDownstream.fail(new RuntimeException("I'm failing"));
        multiUpstreamRowDownstream.finish();

        assertThat(multiUpstreamRowDownstream.pendingUpstreams(), is(0));
        assertThat(multiUpstreamRowDownstream.failure(), instanceOf(RuntimeException.class));
        assertThat(multiUpstreamRowDownstream.allUpstreamsFinishedSuccessful(), is(false));
        assertThat(multiUpstreamRowDownstream.upstreamsRunning(), is(false));
        assertThat(multiUpstreamRowDownstream.setNextRow(new Row1(0)), is(false));
    }

    @Test
    public void testWithDownstreamHandle() throws Exception {
        final AtomicReference<Boolean> rowSet = new AtomicReference<>(false);
        final AtomicReference<Boolean> finishedCalled = new AtomicReference<>(false);

        RowDownstreamHandle downstreamHandle = new RowDownstreamHandle() {
            @Override
            public boolean setNextRow(Row row) {
                rowSet.set(true);
                return true;
            }

            @Override
            public void finish() {
                finishedCalled.set(true);
            }

            @Override
            public void fail(Throwable throwable) {
            }
        };

        multiUpstreamRowDownstream.downstreamHandle(downstreamHandle);
        assertThat(multiUpstreamRowDownstream.downstreamHandle(), is(downstreamHandle));

        multiUpstreamRowDownstream.registerUpstream(null);
        multiUpstreamRowDownstream.setNextRow(new Row1(0));
        assertThat(rowSet.get(), is(true));

        multiUpstreamRowDownstream.finish();
        assertThat(finishedCalled.get(), is(true));
    }

    @Test
    public void testFailureWithDownstreamHandle() throws Exception {
        final AtomicReference<Boolean> finishedCalled = new AtomicReference<>(false);
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        RowDownstreamHandle downstreamHandle = new RowDownstreamHandle() {
            @Override
            public boolean setNextRow(Row row) {
                return true;
            }

            @Override
            public void finish() {
                finishedCalled.set(true);
            }

            @Override
            public void fail(Throwable throwable) {
                failure.set(throwable);
            }
        };

        multiUpstreamRowDownstream.downstreamHandle(downstreamHandle);
        multiUpstreamRowDownstream.registerUpstream(null);
        multiUpstreamRowDownstream.registerUpstream(null);
        multiUpstreamRowDownstream.fail(new RuntimeException("I'm failing"));
        assertThat(failure.get(), instanceOf(RuntimeException.class));

        multiUpstreamRowDownstream.finish();
        assertThat(finishedCalled.get(), is(false));
    }

    @Test
    public void testWithDownstreamHandleProxy() throws Exception {
        final AtomicReference<Boolean> rowSet = new AtomicReference<>(false);
        final AtomicReference<Boolean> finishedCalled = new AtomicReference<>(false);
        RowDownstreamHandle downstreamHandleProxy =
                new RowDownstreamHandle() {
                    @Override
                    public void finish() {
                        finishedCalled.set(true);
                    }

                    @Override
                    public void fail(Throwable t) {
                    }

                    @Override
                    public boolean setNextRow(Row row) {
                        rowSet.set(true);
                        return true;
                    }
                };

        multiUpstreamRowDownstream.downstreamHandleProxy(downstreamHandleProxy);
        multiUpstreamRowDownstream.registerUpstream(null);
        multiUpstreamRowDownstream.setNextRow(new Row1(0));
        assertThat(rowSet.get(), is(true));

        multiUpstreamRowDownstream.finish();
        assertThat(finishedCalled.get(), is(true));
    }
}
