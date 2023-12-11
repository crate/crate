/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.jobs;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.role.Role;

public class TasksServiceTest extends CrateDummyClusterServiceUnitTest {

    private TasksService tasksService;

    @Before
    public void prepare() {
        JobsLogs jobsLogs = new JobsLogs(() -> true);
        tasksService = new TasksService(clusterService, jobsLogs);
    }

    @After
    public void cleanUp() throws Exception {
        tasksService.close();
    }

    @Test
    public void testAcquireContext() throws Exception {
        // create new context
        UUID jobId = UUID.randomUUID();
        RootTask.Builder builder1 = tasksService.newBuilder(jobId);
        Task subContext = new DummyTask();
        builder1.addTask(subContext);
        RootTask ctx1 = tasksService.createTask(builder1);
        assertThat(ctx1.getTask(1), is(subContext));
    }

    @Test
    public void testGetContextsByCoordinatorNode() throws Exception {
        RootTask.Builder builder = tasksService.newBuilder(UUID.randomUUID());
        builder.addTask(new DummyTask(1));

        RootTask ctx = tasksService.createTask(builder);
        Iterable<UUID> contexts = tasksService.getJobIdsByCoordinatorNode("wrongNodeId").collect(Collectors.toList());

        assertThat(contexts.iterator().hasNext(), is(false));

        contexts = tasksService.getJobIdsByCoordinatorNode("n1").collect(Collectors.toList());
        assertThat(contexts, contains(ctx.jobId()));
    }

    @Test
    public void testAcquireContextSameJobId() throws Exception {
        UUID jobId = UUID.randomUUID();

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(String.format(Locale.ENGLISH,
            "task for job %s already exists", jobId));

        // create new context
        RootTask.Builder builder1 = tasksService.newBuilder(jobId);
        builder1.addTask(new DummyTask(1));
        tasksService.createTask(builder1);

        // creating a context with the same jobId will fail
        RootTask.Builder builder2 = tasksService.newBuilder(jobId);
        builder2.addTask(new DummyTask(2));

        tasksService.createTask(builder2);
    }

    @Test
    public void testCreateCallWithEmptyBuilderThrowsAnError() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("RootTask.Builder must at least contain 1 Task");

        RootTask.Builder builder = tasksService.newBuilder(UUID.randomUUID());
        tasksService.createTask(builder);
    }

    @Test
    public void testKillAllCallsKillOnSubContext() throws Exception {
        final AtomicBoolean killCalled = new AtomicBoolean(false);
        Task dummyContext = new DummyTask() {

            @Override
            public void innerKill(@NotNull Throwable throwable) {
                killCalled.set(true);
            }
        };

        RootTask.Builder builder = tasksService.newBuilder(UUID.randomUUID());
        builder.addTask(dummyContext);
        tasksService.createTask(builder);

        Field activeTasksField = TasksService.class.getDeclaredField("activeTasks");
        activeTasksField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<UUID, RootTask> activeTasks = (Map<UUID, RootTask>) activeTasksField.get(tasksService);
        assertThat(activeTasks.size(), is(1));
        assertThat(tasksService.killAll(Role.CRATE_USER.name()).get(5L, TimeUnit.SECONDS), is(1));

        assertThat(killCalled.get(), is(true));
        assertThat(activeTasks.size(), is(0));
    }

    @Test
    public void testKillJobsCallsKillOnSubContext() throws Exception {
        final AtomicBoolean killCalled = new AtomicBoolean(false);
        final AtomicBoolean kill2Called = new AtomicBoolean(false);
        Task dummyContext = new DummyTask() {

            @Override
            public void innerKill(@NotNull Throwable throwable) {
                killCalled.set(true);
            }
        };

        UUID jobId = UUID.randomUUID();
        RootTask.Builder builder = tasksService.newBuilder(jobId);
        builder.addTask(dummyContext);
        tasksService.createTask(builder);

        builder = tasksService.newBuilder(UUID.randomUUID());
        builder.addTask(new DummyTask() {
            @Override
            public void innerKill(@NotNull Throwable throwable) {
                kill2Called.set(true);
            }
        });
        tasksService.createTask(builder);

        Field activeTasksField = TasksService.class.getDeclaredField("activeTasks");
        activeTasksField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<UUID, RootTask> activeTasks = (Map<UUID, RootTask>) activeTasksField.get(tasksService);
        assertThat(activeTasks.size(), is(2));
        assertThat(tasksService.killJobs(List.of(jobId), Role.CRATE_USER.name(), null).get(5L, TimeUnit.SECONDS), is(1));

        assertThat(killCalled.get(), is(true));
        assertThat(kill2Called.get(), is(false));
        assertThat(activeTasks.size(), is(1)); //only one job is killed

    }


    private int numContexts(RootTask rootTask) throws Exception {
        Field orderedTasks = RootTask.class.getDeclaredField("orderedTasks");
        orderedTasks.setAccessible(true);
        return (int) ((List<Task>)(List) orderedTasks.get(rootTask)).stream().filter(x -> !x.completionFuture().isDone()).count();
    }

    @Test
    public void testJobExecutionContextIsSelfClosing() throws Exception {
        RootTask.Builder builder1 = tasksService.newBuilder(UUID.randomUUID());
        DummyTask subContext = new DummyTask();

        builder1.addTask(subContext);
        RootTask ctx1 = tasksService.createTask(builder1);

        assertThat(numContexts(ctx1), is(1));
        subContext.close();
        assertThat(numContexts(ctx1), is(0));
    }

    @Test
    public void testKillReturnsNumberOfJobsKilled() throws Exception {
        RootTask.Builder builder = tasksService.newBuilder(UUID.randomUUID());
        builder.addTask(new DummyTask(1));
        builder.addTask(new DummyTask(2));
        builder.addTask(new DummyTask(3));
        builder.addTask(new DummyTask(4));
        tasksService.createTask(builder);
        builder = tasksService.newBuilder(UUID.randomUUID());
        builder.addTask(new DummyTask(1));
        tasksService.createTask(builder);

        assertThat(tasksService.killAll(Role.CRATE_USER.name()).get(), is(2));
    }

    @Test
    public void testKillSingleJob() throws Exception {
        List<UUID> jobsToKill = List.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        RootTask.Builder builder = tasksService.newBuilder(jobsToKill.get(0));
        builder.addTask(new DummyTask());
        tasksService.createTask(builder);

        builder = tasksService.newBuilder(UUID.randomUUID());
        builder.addTask(new DummyTask());
        tasksService.createTask(builder);

        builder = tasksService.newBuilder(UUID.randomUUID());
        builder.addTask(new DummyTask());
        tasksService.createTask(builder);
        assertThat(tasksService.killJobs(jobsToKill, Role.CRATE_USER.name(), null).get(5L, TimeUnit.SECONDS), is(1));
    }

    @Test
    public void testNormalUserCannotKillJobOfOtherUser() throws Exception {
        UUID jobId = UUID.randomUUID();
        RootTask.Builder builder = tasksService.newBuilder(jobId, "Arthur", "dummyNode", List.of());
        builder.addTask(new DummyTask());
        tasksService.createTask(builder);

        assertThat(tasksService.killJobs(List.of(jobId), "Trillian", null).get(5L, TimeUnit.SECONDS), is(0));
        assertThat(tasksService.killJobs(List.of(jobId), "Arthur", null).get(5L, TimeUnit.SECONDS), is(1));
    }

    @Test
    public void testKillNonExistingJobForNormalUser() throws Exception {
        assertThat(tasksService.killJobs(List.of(UUID.randomUUID()), "Arthur", null).get(5L, TimeUnit.SECONDS), is(0));
    }
}
