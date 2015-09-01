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
import org.elasticsearch.action.bulk.SymbolBasedBulkShardProcessor;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CancellationException;

import static org.mockito.Mockito.*;

public class SymbolBasedBulkShardProcessorContextTest extends CrateUnitTest {

    private ContextCallback callback;
    private SymbolBasedBulkShardProcessorContext context;
    private SymbolBasedBulkShardProcessor processor;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        processor = mock(SymbolBasedBulkShardProcessor.class);
        context = new SymbolBasedBulkShardProcessorContext(processor);
        callback = mock(ContextCallback.class);
        context.addCallback(callback);
    }


    @Test
    public void testKill() throws Exception {
        context.start();
        verify(processor, times(1)).close();

        context.kill(null);
        verify(processor, times(1)).kill(any(CancellationException.class));
        verify(callback, times(1)).onKill();
    }

    @Test
    public void testCloseAfterKill() throws Exception {
        context.start();
        context.kill(null);
        context.close();
        // BulkShardProcessor is killed and callback is closed once
        verify(processor, times(1)).kill(any(CancellationException.class));
        verify(callback, times(1)).onKill();
    }

    @Test
    public void testStartAfterKill() throws Exception {
        context.kill(null);
        verify(processor, times(1)).kill(any(CancellationException.class));
        verify(callback, times(1)).onKill();

        // close is never called on BulkShardProcessor so no requests are issued
        context.start();
        verify(processor, never()).close();
    }
}
