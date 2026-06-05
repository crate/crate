/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.junit.Test;

import io.crate.execution.jobs.PageBucketReceiver;
import io.crate.execution.jobs.transport.JobResponse;

public class BucketForwarderTest {

    @Test
    public void test_no_direct_response_mode_no_forwarding() {
        var initTracker = new InitializationTracker(1);
        var receiver = mock(PageBucketReceiver.class);
        var jobResponse = new JobResponse(List.of());

        var forwarder = new BucketForwarder(List.of(receiver), 0, initTracker);
        forwarder.onResponse(jobResponse);

        verify(receiver, never()).setBucket(anyInt(), any(), anyBoolean(), any());
        assertThat(initTracker.countdownFuture()).isCompleted();
    }

}
