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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Literal;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchConsumer;
import io.crate.data.BatchIterator;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.RowGranularity;
import io.crate.operation.InputFactory;
import io.crate.operation.operator.EqOperator;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.types.DataTypes;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.MockitoAnnotations;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class ProjectingBatchConsumerTest {

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));
    private Functions functions;
    private ThreadPool threadPool;
    private ProjectorFactory projectorFactory;


    @Before
    public void prepare() {
        MockitoAnnotations.initMocks(this);
        functions = getFunctions();
        threadPool = new ThreadPool("testing");
        projectorFactory = new ProjectionToProjectorVisitor(
            mock(ClusterService.class),
            functions,
            new IndexNameExpressionResolver(Settings.EMPTY),
            threadPool,
            Settings.EMPTY,
            mock(TransportActionProvider.class, Answers.RETURNS_DEEP_STUBS.get()),
            mock(BulkRetryCoordinatorPool.class),
            new InputFactory(functions),
            EvaluatingNormalizer.functionOnlyNormalizer(functions, ReplaceMode.COPY),
            null
        );
    }

    @After
    public void after() throws Exception {
        threadPool.shutdown();
        threadPool.awaitTermination(1, TimeUnit.SECONDS);
    }

    private static class DummyBatchConsumer implements BatchConsumer {

        private final boolean requiresScroll;

        DummyBatchConsumer(boolean requiresScroll) {
            this.requiresScroll = requiresScroll;
        }

        @Override
        public void accept(BatchIterator iterator, @Nullable Throwable failure) {
        }

        @Override
        public boolean requiresScroll() {
            return requiresScroll;
        }
    }


    @Test
    public void testConsumerRequiresScrollAndProjectorsDontSupportScrolling() throws Exception {
        EqOperator op = (EqOperator) functions.get(
            new FunctionIdent(EqOperator.NAME, ImmutableList.of(DataTypes.INTEGER, DataTypes.INTEGER)));
        Function function = new Function(op.info(), Arrays.asList(Literal.of(2), new InputColumn(1)));
        FilterProjection filterProjection = new FilterProjection(function,
            Arrays.asList(new InputColumn(0), new InputColumn(1)));

        BatchConsumer delegateConsumerRequiresScroll = new DummyBatchConsumer(true);

        BatchConsumer projectingConsumer = ProjectingBatchConsumer.create(delegateConsumerRequiresScroll,
            Collections.singletonList(filterProjection), UUID.randomUUID(), RAM_ACCOUNTING_CONTEXT, projectorFactory);

        assertThat(projectingConsumer.requiresScroll(), is(true));
    }

    @Test
    public void testConsumerRequiresScrollAndProjectorsSupportScrolling() throws Exception {
        GroupProjection groupProjection = new GroupProjection(new ArrayList<>(), new ArrayList<>(), RowGranularity.DOC);

        BatchConsumer delegateConsumerRequiresScroll = new DummyBatchConsumer(true);

        BatchConsumer projectingConsumer = ProjectingBatchConsumer.create(delegateConsumerRequiresScroll,
            Collections.singletonList(groupProjection), UUID.randomUUID(), RAM_ACCOUNTING_CONTEXT, projectorFactory);

        assertThat(projectingConsumer.requiresScroll(), is(false));
    }

    @Test
    public void testConsumerDoesNotRequireScrollYieldsProjectingConsumerWithoutScrollRequirements() throws Exception {
        GroupProjection groupProjection = new GroupProjection(new ArrayList<>(), new ArrayList<>(), RowGranularity.DOC);
        BatchConsumer delegateConsumerRequiresScroll = new DummyBatchConsumer(false);

        BatchConsumer projectingConsumer = ProjectingBatchConsumer.create(delegateConsumerRequiresScroll,
            Collections.singletonList(groupProjection), UUID.randomUUID(), RAM_ACCOUNTING_CONTEXT, projectorFactory);

        assertThat(projectingConsumer.requiresScroll(), is(false));
    }
}
