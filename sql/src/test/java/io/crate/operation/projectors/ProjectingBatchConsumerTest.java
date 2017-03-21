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

import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Literal;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchConsumer;
import io.crate.data.RowsBatchIterator;
import io.crate.exceptions.UnhandledServerException;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.RowGranularity;
import io.crate.operation.InputFactory;
import io.crate.planner.projection.WriterProjection;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingBatchConsumer;
import io.crate.types.DataTypes;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.TestingHelpers.getFunctions;
import static org.mockito.Mockito.mock;

public class ProjectingBatchConsumerTest extends CrateUnitTest {

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));
    private Functions functions;
    private ThreadPool threadPool;
    private ProjectorFactory projectorFactory;


    @Before
    public void prepare() {
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
            new EvaluatingNormalizer(
                functions,
                RowGranularity.SHARD,
                ReplaceMode.COPY,
                r -> Literal.of(r.valueType(), r.valueType().value("1")),
                null),
            null,
            new ShardId("dummy", 0)
        );
    }

    @After
    public void after() throws Exception {
        threadPool.shutdown();
        threadPool.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    public void testErrorHandlingIfProjectorApplicationFails() throws Exception {
        WriterProjection writerProjection = new WriterProjection(
            Collections.singletonList(new InputColumn(0, DataTypes.STRING)),
            Literal.of("/x/y/z/hopefully/invalid/on/your/system/"),
            null,
            Collections.emptyMap(),
            Collections.emptyList(),
            WriterProjection.OutputFormat.JSON_OBJECT);

        TestingBatchConsumer consumer = new TestingBatchConsumer();
        BatchConsumer batchConsumer = ProjectingBatchConsumer.create(
            consumer,
            Collections.singletonList(writerProjection),
            UUID.randomUUID(),
            RAM_ACCOUNTING_CONTEXT,
            projectorFactory
        );

        batchConsumer.accept(RowsBatchIterator.empty(), null);

        expectedException.expect(UnhandledServerException.class);
        expectedException.expectMessage("Failed to open output");
        consumer.getResult();
    }
}
