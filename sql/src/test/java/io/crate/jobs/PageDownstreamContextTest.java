/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.jobs;

import io.crate.Streamer;
import io.crate.core.collections.Row1;
import io.crate.core.collections.SingleRowBucket;
import io.crate.operation.PageDownstream;
import io.crate.operation.PageResultListener;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.Mockito.*;

public class PageDownstreamContextTest extends CrateUnitTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testCantSetSameBucketTwiceWithoutReceivingFullPage() throws Exception {
        expectedException.expect(IllegalStateException.class);
        PageDownstreamContext ctx = new PageDownstreamContext(mock(PageDownstream.class), new Streamer[0], 3);

        PageResultListener pageResultListener = mock(PageResultListener.class);
        ctx.setBucket(1, new SingleRowBucket(new Row1("foo")), false, pageResultListener);
        ctx.setBucket(1, new SingleRowBucket(new Row1("foo")), false, pageResultListener);
    }

    @Test
    public void testKill() throws Exception {
        PageDownstream downstream = mock(PageDownstream.class);
        ContextCallback callback = mock(ContextCallback.class);

        PageDownstreamContext ctx = new PageDownstreamContext(downstream, new Streamer[0], 3);
        ctx.addCallback(callback);
        ctx.kill();

        verify(callback, times(1)).onClose();
        ArgumentCaptor<JobKilledException> e = ArgumentCaptor.forClass(JobKilledException.class);
        verify(downstream, times(1)).fail(e.capture());
        assertThat(e.getValue(), instanceOf(JobKilledException.class));
    }
}