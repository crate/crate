/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.task;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.operator.operations.collect.LocalDataCollectOperation;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.CollectNode;
import io.crate.planner.symbol.DoubleLiteral;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LocalCollectTaskTest {

    public static Routing CLUSTER_ROUTING = new Routing();
    private static Object[][] TEST_RESULT = new Object[][] {
            new Object[] {"foobarbaz", 4.5d}
    };

    private UUID testJobId = UUID.randomUUID();
    private LocalDataCollectOperation collectOperation;

    static class TestModule extends AbstractModule {

        @Override
        protected void configure() {
            LocalDataCollectOperation collectOperation = mock(LocalDataCollectOperation.class);
            SettableFuture<Object[][]> mockedFuture = SettableFuture.create();
            mockedFuture.set(TEST_RESULT);
            when(collectOperation.collect(any(CollectNode.class))).thenReturn(mockedFuture);
            bind(LocalDataCollectOperation.class).toInstance(collectOperation);
        }
    }

    @Before
    public void prepare() {
        Injector injector = new ModulesBuilder()
                .add(new TestModule())
                .createInjector();
        collectOperation = injector.getInstance(LocalDataCollectOperation.class);
    }

    @Test
    public void testCollectTask() throws Exception {
        CollectNode collectNode = new CollectNode("ei-die", CLUSTER_ROUTING);
        collectNode.maxRowGranularity(RowGranularity.CLUSTER);
        collectNode.jobId(testJobId);
        collectNode.toCollect(Arrays.<Symbol>asList(
                new Reference(new ReferenceInfo(
                        new ReferenceIdent(
                            new TableIdent("foo", "bar"), "baz"),
                            RowGranularity.CLUSTER,
                            DataType.STRING)),
                new DoubleLiteral(4.5d)));

        LocalCollectTask collectTask = new LocalCollectTask(collectOperation, collectNode);
        collectTask.start();
        List<ListenableFuture<Object[][]>> results = collectTask.result();
        assertThat(results.size(), is(1));

        ListenableFuture<Object[][]> result = results.get(0);
        assertThat(result.get(), is(TEST_RESULT));
    }
}
