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

import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.BatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowsBatchIterator;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.executor.transport.TransportShardUpsertAction;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.operation.collect.RowShardResolver;
import io.crate.testing.BatchIteratorTester;
import io.crate.testing.RowGenerator;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.create.TransportBulkCreateIndicesAction;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class IndexWriterCountBatchIteratorTest extends SQLTransportIntegrationTest {

    private static final ColumnIdent ID_IDENT = new ColumnIdent("id");
    private static final TableIdent bulkImportIdent = new TableIdent(null, "bulk_import");

    @Test
    public void testIndexWriterIterator() throws Exception {
        execute("create table bulk_import (id int primary key) with (number_of_replicas=0)");
        ensureGreen();
        BatchIterator source = RowsBatchIterator.newInstance(RowGenerator.fromSingleColValues(
            () -> IntStream.range(0, 10).mapToObj(i -> new BytesRef("{\"id\": " + i + "}")).iterator()
        ), 1);

        Supplier<String> indexNameResolver = IndexNameResolver.forTable(new TableIdent(null, "bulk_import"));
        Input<?> sourceInput = new InputCollectExpression(0);

        List<CollectExpression<Row, ?>> collectExpressions =
            Collections.singletonList((InputCollectExpression) sourceInput);

        RowShardResolver rowShardResolver = getRowShardResolver();
        BulkShardProcessor bulkShardProcessor = getBulkShardProcessor();

        BatchIteratorTester tester = new BatchIteratorTester(() ->
            IndexWriterCountBatchIterator.newInstance(source, indexNameResolver, (Input<BytesRef>) sourceInput,
                collectExpressions, rowShardResolver, bulkShardProcessor),
            Collections.singletonList(new Object[]{10L}));
        tester.run();
    }

    private RowShardResolver getRowShardResolver() {
        return new RowShardResolver(internalCluster().getInstance(Functions.class),
            Arrays.asList(ID_IDENT),
            Arrays.<Symbol>asList(new InputColumn(0)), null, null);
    }

    private BulkShardProcessor<ShardUpsertRequest> getBulkShardProcessor() {
        UUID jobId = UUID.randomUUID();
        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
            CrateSettings.BULK_REQUEST_TIMEOUT.extractTimeValue(Settings.EMPTY),
            false,
            true,
            null,
            new Reference[]{new Reference(new ReferenceIdent(bulkImportIdent, DocSysColumns.RAW), RowGranularity.DOC, DataTypes.STRING)},
            jobId,
            false);

        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(Settings.EMPTY);
        return new BulkShardProcessor<>(
            internalCluster().getInstance(ClusterService.class),
            internalCluster().getInstance(TransportBulkCreateIndicesAction.class),
            indexNameExpressionResolver,
            Settings.EMPTY,
            internalCluster().getInstance(BulkRetryCoordinatorPool.class),
            false,
            2,
            builder,
            internalCluster().getInstance(TransportShardUpsertAction.class)::execute,
            jobId
        );
    }

}
