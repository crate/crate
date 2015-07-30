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

import io.crate.test.integration.CrateUnitTest;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;

public class MultiUpstreamRowUpstreamTest extends CrateUnitTest {

    MultiUpstreamRowDownstream multiUpstreamRowDownstream;
    MultiUpstreamRowUpstream multiUpstreamRowUpstream;

    @Before
    public void before() {
        multiUpstreamRowDownstream = new MultiUpstreamRowDownstream();
        multiUpstreamRowUpstream = new MultiUpstreamRowUpstream(multiUpstreamRowDownstream);
    }

    @Test
    public void testPauseResumeBubbling() throws Exception {
        final AtomicInteger pause = new AtomicInteger(0);
        final AtomicInteger resume = new AtomicInteger(0);

        RowUpstream rowUpstream = new RowUpstream() {
            @Override
            public void pause() {
                pause.incrementAndGet();
            }

            @Override
            public void resume() {
                resume.incrementAndGet();
            }
        };

        multiUpstreamRowDownstream.registerUpstream(rowUpstream);
        multiUpstreamRowDownstream.registerUpstream(rowUpstream);

        multiUpstreamRowUpstream.pause();
        assertThat(pause.get(), is(2));

        multiUpstreamRowUpstream.resume();
        assertThat(resume.get(), is(2));
    }
}
