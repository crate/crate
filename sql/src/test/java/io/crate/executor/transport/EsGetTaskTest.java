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

package io.crate.executor.transport;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.Job;
import io.crate.executor.Page;
import io.crate.executor.Task;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.task.elasticsearch.ESGetTask;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.TableIdent;
import io.crate.planner.IterablePlan;
import io.crate.planner.Plan;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.ESGetNode;
import io.crate.planner.symbol.DynamicReference;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.TestingHelpers;
import org.junit.Test;
import rx.Observable;
import rx.Observer;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class EsGetTaskTest extends BaseTransportExecutorTest {

    private ESGetTask newGetTask(String tableName, List<Symbol> outputs, String ... ids) {
        ESGetNode node = newGetNode(tableName, outputs, Arrays.asList(ids));
        Plan plan = new IterablePlan(node);
        Job job = executor.newJob(plan);
        assertThat(job.tasks().size(), is(1));
        Task task = job.tasks().get(0);
        assertThat(task, instanceOf(ESGetTask.class));
        return (ESGetTask)task;
    }

    @Test
    public void testESGetTask() throws Exception {
        setup.setUpCharacters();

        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        ESGetNode node = newGetNode("characters", outputs, "2");
        Plan plan = new IterablePlan(node);
        Job job = executor.newJob(plan);
        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        Object[][] objects = result.get(0).get().rows();

        assertThat(objects.length, is(1));
        assertThat((Integer) objects[0][0], is(2));
        assertThat((String) objects[0][1], is("Ford"));
    }

    @Test
    public void testESGetTaskAsObservable() throws Exception {
        setup.setUpCharacters();

        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        ESGetTask esGetTask = newGetTask("characters", outputs, "2");
        Observable<Page> observable = esGetTask.asObservable();
        final AtomicReference<Page> page = new AtomicReference<>();
        final SettableFuture<Void> observableFuture = SettableFuture.create();
        observable.subscribe(new Observer<Page>() {
            @Override
            public void onCompleted() {
                observableFuture.set(null);
            }

            @Override
            public void onError(Throwable e) {
                Throwables.propagate(e);
            }

            @Override
            public void onNext(Page objects) {
                page.set(objects);
            }
        });
        observableFuture.get();
        assertThat(TestingHelpers.printedPage(page.get()), is("2| Ford\n"));
    }

    @Test
    public void testESGetTaskWithDynamicReference() throws Exception {
        setup.setUpCharacters();

        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, new DynamicReference(
                new ReferenceIdent(new TableIdent(null, "characters"), "foo"), RowGranularity.DOC));
        ESGetNode node = newGetNode("characters", outputs, "2");
        Plan plan = new IterablePlan(node);
        Job job = executor.newJob(plan);
        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        Object[][] objects = result.get(0).get().rows();

        assertThat(objects.length, is(1));
        assertThat((Integer) objects[0][0], is(2));
        assertNull(objects[0][1]);
    }

    @Test
    public void testESMultiGet() throws Exception {
        setup.setUpCharacters();
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        ESGetNode node = newGetNode("characters", outputs, asList("1", "2"));
        Plan plan = new IterablePlan(node);
        Job job = executor.newJob(plan);
        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        Object[][] objects = result.get(0).get().rows();

        assertThat(objects.length, is(2));
    }

    @Test
    public void testESMultiGetAsObservable() throws Exception {
        setup.setUpCharacters();
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        ESGetTask task = newGetTask("characters", outputs, "4", "3");
        Observable<Page> observable = task.asObservable();

        final AtomicReference<Page> page = new AtomicReference<>();
        final SettableFuture<Void> observableFuture = SettableFuture.create();
        observable.subscribe(new Observer<Page>() {
            @Override
            public void onCompleted() {
                observableFuture.set(null);
            }

            @Override
            public void onError(Throwable e) {
                Throwables.propagate(e);
            }

            @Override
            public void onNext(Page objects) {
                page.set(objects);
            }
        });
        observableFuture.get();
        assertThat(TestingHelpers.printedPage(page.get()), is(
                "4| Arthur\n" +
                "3| Trillian\n"));
    }
}
