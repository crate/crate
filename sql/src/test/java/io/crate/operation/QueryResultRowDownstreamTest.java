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

package io.crate.operation;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.TaskResult;
import io.crate.jobs.JobContextService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class QueryResultRowDownstreamTest {

    @Test
    public void testContextIsClosedOnFinish() throws Exception {
        Settings settings = ImmutableSettings.EMPTY;
        JobContextService jobContextService = new JobContextService(settings, new ThreadPool("foo"));

        UUID jobId = UUID.randomUUID();
        jobContextService.getOrCreateContext(jobId);

        SettableFuture<TaskResult> result = SettableFuture.create();
        QueryResultRowDownstream rowDownstream = new QueryResultRowDownstream(result, jobId, 1, jobContextService);

        RowDownstreamHandle rowDownstreamHandle = rowDownstream.registerUpstream(null);
        rowDownstreamHandle.finish();

        assertThat(jobContextService.getContext(jobId), nullValue());
    }
}