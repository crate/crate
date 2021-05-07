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

package io.crate.execution.engine.indexing;

import io.crate.breaker.RamAccounting;
import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.InputCollectExpression;
import io.crate.execution.jobs.NodeLimits;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.TestingRowConsumer;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.create.TransportCreatePartitionsAction;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static io.crate.data.SentinelRow.SENTINEL;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class IndexWriterProjectorUnitTest extends CrateDummyClusterServiceUnitTest {

    private static final ColumnIdent ID_IDENT = new ColumnIdent("id");
    private static final RelationName BULK_IMPORT_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "bulk_import");
    private static final Reference RAW_SOURCE_REFERENCE = new Reference(
        new ReferenceIdent(BULK_IMPORT_IDENT, "_raw"),
        RowGranularity.DOC,
        DataTypes.STRING,
        0,
        null);

    private ExecutorService executor;
    private ScheduledExecutorService scheduler;

    @Before
    public void setUpExecutors() throws Exception {
        scheduler = Executors.newScheduledThreadPool(1);
        executor = Executors.newFixedThreadPool(1);
    }

    @After
    public void tearDownExecutors() throws Exception {
        scheduler.shutdown();
        executor.shutdown();
        scheduler.awaitTermination(10, TimeUnit.SECONDS);
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testNullPKValue() throws Throwable {
        InputCollectExpression sourceInput = new InputCollectExpression(0);
        List<CollectExpression<Row, ?>> collectExpressions = Collections.<CollectExpression<Row, ?>>singletonList(sourceInput);

        TransportCreatePartitionsAction transportCreatePartitionsAction = mock(TransportCreatePartitionsAction.class);
        IndexWriterProjector indexWriter = new IndexWriterProjector(
            clusterService,
            new NodeLimits(),
            new NoopCircuitBreaker("dummy"),
            RamAccounting.NO_ACCOUNTING,
            scheduler,
            executor,
            CoordinatorTxnCtx.systemTransactionContext(),
            createNodeContext(),
            Settings.EMPTY,
            5,
            1,
            transportCreatePartitionsAction,
            (request, listener) -> {},
            IndexNameResolver.forTable(BULK_IMPORT_IDENT),
            RAW_SOURCE_REFERENCE,
            Collections.singletonList(ID_IDENT),
            Collections.<Symbol>singletonList(new InputColumn(1)),
            null,
            null,
            sourceInput,
            collectExpressions,
            20,
            null,
            null,
            false,
            false,
            UUID.randomUUID(),
            UpsertResultContext.forRowCount());

        RowN rowN = new RowN(new Object[]{new BytesRef("{\"y\": \"x\"}"), null});
        BatchIterator<Row> batchIterator = InMemoryBatchIterator.of(Collections.singletonList(rowN), SENTINEL, true);
        batchIterator = indexWriter.apply(batchIterator);

        TestingRowConsumer testingBatchConsumer = new TestingRowConsumer();
        testingBatchConsumer.accept(batchIterator, null);

        List<Object[]> result = testingBatchConsumer.getResult();
        // Zero affected rows as a NULL as a PK value will result in an exception.
        // It must never bubble up as other rows might already have been written.
        assertThat(result.get(0)[0], is(0L));
    }
}
