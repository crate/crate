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

package io.crate.operation.projectors;

import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.*;
import io.crate.executor.transport.TransportShardUpsertAction;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.testing.CollectingRowReceiver;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.create.TransportBulkCreateIndicesAction;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.Is.is;

public class IndexWriterProjectorTest extends SQLTransportIntegrationTest {

    private static final ColumnIdent ID_IDENT = new ColumnIdent("id");

    private static final TableIdent bulkImportIdent = new TableIdent(null, "bulk_import");

    @Test
    public void testIndexWriter() throws Throwable {
        execute("create table bulk_import (id int primary key, name string) with (number_of_replicas=0)");
        ensureGreen();

        CollectingRowReceiver collectingRowReceiver = new CollectingRowReceiver();
        InputCollectExpression sourceInput = new InputCollectExpression(1);
        List<CollectExpression<Row, ?>> collectExpressions = Collections.<CollectExpression<Row, ?>>singletonList(sourceInput);

        IndexWriterProjector writerProjector = new IndexWriterProjector(
            internalCluster().getInstance(ClusterService.class),
            internalCluster().getInstance(Functions.class),
            new IndexNameExpressionResolver(Settings.EMPTY),
            Settings.EMPTY,
            internalCluster().getInstance(TransportBulkCreateIndicesAction.class),
            internalCluster().getInstance(TransportShardUpsertAction.class)::execute,
            IndexNameResolver.forTable(new TableIdent(null, "bulk_import")),
            internalCluster().getInstance(BulkRetryCoordinatorPool.class),
            new Reference(new ReferenceIdent(bulkImportIdent, DocSysColumns.RAW), RowGranularity.DOC, DataTypes.STRING),
            Arrays.asList(ID_IDENT),
            Arrays.<Symbol>asList(new InputColumn(0)),
            null,
            null,
            sourceInput,
            collectExpressions,
            20,
            null,
            null,
            false,
            false,
            UUID.randomUUID()
        );
        writerProjector.downstream(collectingRowReceiver);

        BatchConsumerToRowReceiver batchConsumer = new BatchConsumerToRowReceiver(writerProjector);
        BatchIterator rowsIterator = RowsBatchIterator.newInstance(IntStream.range(0, 100)
            .mapToObj(i -> new RowN(new Object[]{i, new BytesRef("{\"id\": " + i + ", \"name\": \"Arthur\"}")}))
            .collect(Collectors.toList()), 2);

        batchConsumer.accept(rowsIterator, null);
        Bucket objects = collectingRowReceiver.result();

        assertThat(objects, contains(isRow(100L)));

        execute("refresh table bulk_import");
        execute("select count(*) from bulk_import");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is(100L));
    }
}
