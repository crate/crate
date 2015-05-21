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

package io.crate.executor.transport.task;

import com.google.common.util.concurrent.Futures;
import io.crate.executor.TaskResult;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.fail;

public class AbstractChainedTaskTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testErrorHandling() throws Exception {
        DummyTask t = new DummyTask(UUID.randomUUID());

        try {
            t.upstreamResult(Collections.singletonList(Futures.<TaskResult>immediateFailedFuture(new IllegalArgumentException("test"))));
            t.start();
        } catch (Exception e) {
            fail("start() shouldn't throw an exception");
        }

        expectedException.expectCause(Matchers.any(IllegalArgumentException.class));
        t.result().get(0).get();
    }

    private static class DummyTask extends AbstractChainedTask {

        protected DummyTask(UUID jobId) {
            super(jobId);
        }

        @Override
        protected void doStart(List<TaskResult> upstreamResults) {
        }
    }
}