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


import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class BulkShardProcessorContextTest extends CrateUnitTest {

    private BulkShardProcessorContext context;
    private BulkShardProcessor processor;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        processor = mock(BulkShardProcessor.class);
        context = new BulkShardProcessorContext(1, processor);
    }

    @Test
    public void testKill() throws Exception {
        context.prepare();
        context.start();
        verify(processor, times(1)).close();

        context.kill(null);
        verify(processor, times(1)).kill(any(InterruptedException.class));
    }

    @Test
    public void testCloseAfterKill() throws Exception {
        context.prepare();
        context.start();
        context.kill(null);
        context.close();
        // BulkShardProcessor is killed and callback is closed once
        verify(processor, times(1)).kill(any(InterruptedException.class));
    }

    @Test
    public void testStartAfterKill() throws Exception {
        context.prepare();
        context.kill(null);
        verify(processor, times(1)).kill(any(InterruptedException.class));
        // close is never called on BulkShardProcessor so no requests are issued
        context.start();
        verify(processor, never()).close();
    }
}
