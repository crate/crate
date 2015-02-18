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

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.ImmutableList;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.TransportShardUpsertActionDelegateImpl;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.mock;

public class IndexWriterProjectorUnitTest {

    private final static ColumnIdent ID_IDENT = new ColumnIdent("id");
    private static final TableIdent bulkImportIdent = new TableIdent(null, "bulk_import");
    private static Reference rawSourceReference = new Reference(new ReferenceInfo(
            new ReferenceIdent(bulkImportIdent, "_raw"), RowGranularity.DOC, DataTypes.STRING));

    @Mock(answer = Answers.RETURNS_MOCKS)
    ClusterService clusterService;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testExceptionBubbling() throws Throwable {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("my dummy exception");

        CollectingProjector collectingProjector = new CollectingProjector();
        InputCollectExpression<Object> sourceInput = new InputCollectExpression<>(1);
        InputColumn sourceInputColumn = new InputColumn(1);
        CollectExpression[] collectExpressions = new CollectExpression[]{ sourceInput };

        final IndexWriterProjector indexWriter = new IndexWriterProjector(
                clusterService,
                ImmutableSettings.EMPTY,
                mock(TransportShardUpsertActionDelegateImpl.class),
                mock(TransportCreateIndexAction.class),
                "bulk_import",
                rawSourceReference,
                ImmutableList.of(ID_IDENT),
                Arrays.<Symbol>asList(new InputColumn(0)),
                ImmutableList.<Input<?>>of(),
                null,
                sourceInput,
                sourceInputColumn,
                collectExpressions,
                20,
                null, null,
                false,
                false
        );
        indexWriter.downstream(collectingProjector);
        indexWriter.registerUpstream(null);
        indexWriter.upstreamFailed(new IllegalStateException("my dummy exception"));

        try {
            collectingProjector.result().get();
        } catch (InterruptedException | ExecutionException e) {
            throw e.getCause();
        }
    }

    @Test
    public void testNullPKValue() throws Throwable {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A primary key value must not be NULL");

        CollectingProjector collectingProjector = new CollectingProjector();
        InputCollectExpression<Object> sourceInput = new InputCollectExpression<>(0);
        InputColumn sourceInputColumn = new InputColumn(0);
        CollectExpression[] collectExpressions = new CollectExpression[]{ sourceInput };
        final IndexWriterProjector indexWriter = new IndexWriterProjector(
                clusterService,
                ImmutableSettings.EMPTY,
                mock(TransportShardUpsertActionDelegateImpl.class),
                mock(TransportCreateIndexAction.class),
                "bulk_import",
                rawSourceReference,
                ImmutableList.of(ID_IDENT),
                Arrays.<Symbol>asList(new InputColumn(1)),
                ImmutableList.<Input<?>>of(),
                null,
                sourceInput,
                sourceInputColumn,
                collectExpressions,
                20,
                null, null,
                false,
                false
        );
        indexWriter.downstream(collectingProjector);
        indexWriter.registerUpstream(null);
        indexWriter.startProjection();
        indexWriter.setNextRow(new BytesRef("{\"y\": \"x\"}"), null);
        indexWriter.upstreamFinished();
    }
}
