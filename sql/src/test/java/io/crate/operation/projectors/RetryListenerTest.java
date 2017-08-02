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

package io.crate.operation.projectors;

import io.crate.action.LimitedExponentialBackoff;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RetryListenerTest extends CrateUnitTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testStopSchedulingIfJobIsKilled() throws Exception {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        CompletableFuture<Void> executionFuture = new CompletableFuture<>();

        RetryListener<Void> listener = new RetryListener<>(
            scheduler,
            l -> { },
            mock(ActionListener.class),
            LimitedExponentialBackoff.limitedExponential(1000),
            executionFuture);

        executionFuture.complete(null);
        listener.onFailure(new EsRejectedExecutionException());

        verify(scheduler, times(0)).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    }
}
