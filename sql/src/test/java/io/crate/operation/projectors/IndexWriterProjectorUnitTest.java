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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.*;
import io.crate.metadata.*;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingBatchConsumer;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.create.TransportBulkCreateIndicesAction;
import org.elasticsearch.action.bulk.BulkRequestExecutor;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.mock;

public class IndexWriterProjectorUnitTest extends CrateUnitTest {

    private final static ColumnIdent ID_IDENT = new ColumnIdent("id");
    private static final TableIdent bulkImportIdent = new TableIdent(null, "bulk_import");
    private static Reference rawSourceReference = new Reference(
        new ReferenceIdent(bulkImportIdent, "_raw"), RowGranularity.DOC, DataTypes.STRING);

    @Mock(answer = Answers.RETURNS_MOCKS)
    ClusterService clusterService;

    @Test
    public void testNullPKValue() throws Throwable {
        InputCollectExpression sourceInput = new InputCollectExpression(0);
        List<CollectExpression<Row, ?>> collectExpressions = Collections.<CollectExpression<Row, ?>>singletonList(sourceInput);
        final IndexWriterProjector indexWriter = new IndexWriterProjector(
            clusterService,
            TestingHelpers.getFunctions(),
            new IndexNameExpressionResolver(Settings.EMPTY),
            Settings.EMPTY,
            mock(TransportBulkCreateIndicesAction.class),
            mock(BulkRequestExecutor.class),
            () -> "foo",
            mock(BulkRetryCoordinatorPool.class, Answers.RETURNS_DEEP_STUBS.get()),
            rawSourceReference,
            ImmutableList.of(ID_IDENT),
            Arrays.<Symbol>asList(new InputColumn(1)),
            null,
            null,
            sourceInput,
            collectExpressions,
            20,
            null, null,
            false,
            false,
            UUID.randomUUID()
        );

        BatchIteratorProjector projector = indexWriter.asProjector();
        RowN rowN = new RowN(new Object[]{new BytesRef("{\"y\": \"x\"}"), null});
        BatchIterator batchIterator = RowsBatchIterator.newInstance(Collections.singletonList(rowN), rowN.numColumns());
        batchIterator = projector.apply(batchIterator);

        TestingBatchConsumer testingBatchConsumer = new TestingBatchConsumer();
        testingBatchConsumer.accept(batchIterator, null);

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A primary key value must not be NULL");
        testingBatchConsumer.getResult();
    }
}
