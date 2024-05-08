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

import static io.crate.data.SentinelRow.SENTINEL;
import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import io.crate.data.BatchIterator;
import io.crate.data.Bucket;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.data.breaker.RamAccounting;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.RowCollectExpression;
import io.crate.execution.engine.pipeline.TableSettingsResolver;
import io.crate.execution.jobs.NodeLimits;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.settings.NumberOfReplicas;
import io.crate.types.DataTypes;

public class IndexWriterProjectorTest extends IntegTestCase {

    private static final ColumnIdent ID_IDENT = new ColumnIdent("id");


    @Test
    public void testIndexWriter() throws Throwable {
        execute("create table bulk_import (id int primary key, name string) with (number_of_replicas=0)");
        ensureGreen();

        RowCollectExpression sourceInput = new RowCollectExpression(1);
        List<CollectExpression<Row, ?>> collectExpressions = Collections.<CollectExpression<Row, ?>>singletonList(sourceInput);

        RelationName bulkImportIdent = new RelationName(sqlExecutor.getCurrentSchema(), "bulk_import");
        ClusterState state = clusterService().state();
        Settings tableSettings = TableSettingsResolver.get(state.metadata(), bulkImportIdent, false);
        ThreadPool threadPool = cluster().getInstance(ThreadPool.class);
        IndexWriterProjector writerProjector = new IndexWriterProjector(
            clusterService(),
            new NodeLimits(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            new NoopCircuitBreaker("dummy"),
            RamAccounting.NO_ACCOUNTING,
            threadPool.scheduler(),
            threadPool.executor(ThreadPool.Names.SEARCH),
            CoordinatorTxnCtx.systemTransactionContext(),
            new NodeContext(cluster().getInstance(Functions.class), null, clusterService(), null),
            Settings.EMPTY,
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(tableSettings),
            NumberOfReplicas.effectiveNumReplicas(tableSettings, state.nodes()),
            cluster().client(),
            IndexNameResolver.forTable(bulkImportIdent),
            new SimpleReference(new ReferenceIdent(bulkImportIdent, DocSysColumns.RAW),
                          RowGranularity.DOC,
                          DataTypes.STRING,
                          0,
                          null),
            Collections.singletonList(ID_IDENT),
            Collections.<Symbol>singletonList(new InputColumn(0)),
            null,
            null,
            sourceInput,
            collectExpressions,
            20,
            null,
            false,
            false,
            UUID.randomUUID(),
            UpsertResultContext.forRowCount(),
            false
        );

        BatchIterator<Row> rowsIterator = InMemoryBatchIterator.of(IntStream.range(0, 100)
            .mapToObj(i -> new RowN(new Object[]{i, "{\"id\": " + i + ", \"name\": \"Arthur\"}"}))
            .collect(Collectors.toList()), SENTINEL, true);

        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(writerProjector.apply(rowsIterator), null);
        Bucket objects = consumer.getBucket();

        assertThat(objects, contains(isRow(100L)));

        execute("refresh table bulk_import");
        execute("select count(*) from bulk_import");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is(100L));
    }
}
