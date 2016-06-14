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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.JobTask;
import io.crate.executor.TaskResult;
import io.crate.jobs.ESJobContext;
import io.crate.jobs.ExecutionSubContext;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.operation.collect.StatsTables;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;

public class ESJobContextTaskTest extends CrateUnitTest {

    private final JobContextService jobContextService = new JobContextService(
        Settings.EMPTY, new NoopClusterService(), mock(StatsTables.class));

    private EsJobContextTask createTask(UUID jobId) {
        EsJobContextTask task = new EsJobContextTask(jobId, 1, 1, jobContextService);
        task.createContextBuilder("test",
            ImmutableList.of(new DummyRequest()),
            ImmutableList.of(new DummyListener()),
            new TransportAction(Settings.EMPTY, "dummy", mock(ThreadPool.class), new ActionFilters(Collections.EMPTY_SET), mock(IndexNameExpressionResolver.class), mock(TaskManager.class)) {
                @Override
                protected void doExecute(ActionRequest request, ActionListener listener) {
                    // do nothing
                }
            },
            null);
        task.results.add(SettableFuture.<TaskResult>create());
        return task;
    }

    @Test
    public void testContextCreation() throws Exception {
        UUID jobId = UUID.randomUUID();
        EsJobContextTask task = createTask(jobId);
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        task.execute(rowReceiver);

        JobExecutionContext jobExecutionContext = jobContextService.getContext(jobId);
        ExecutionSubContext subContext = jobExecutionContext.getSubContext(1);
        assertThat(subContext, notNullValue());
        assertThat(subContext, instanceOf(ESJobContext.class));
    }

    @Test
    public void testContextKill() throws Exception {
        UUID jobId = UUID.randomUUID();
        JobTask task = createTask(jobId);
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        task.execute(rowReceiver);

        JobExecutionContext jobExecutionContext = jobContextService.getContext(jobId);
        ExecutionSubContext subContext = jobExecutionContext.getSubContext(1);
        subContext.kill(null);

        assertThat(rowReceiver.getNumFailOrFinishCalls(), is(1));
        assertNull(jobExecutionContext.getSubContextOrNull(1));
    }

    static class DummyRequest extends ActionRequest<DummyRequest> {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    static class DummyListener implements ActionListener<Void> {
        @Override
        public void onResponse(Void aVoid) {
        }

        @Override
        public void onFailure(Throwable throwable) {
        }
    }
}
