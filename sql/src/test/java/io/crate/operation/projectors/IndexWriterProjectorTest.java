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

import io.crate.core.collections.Bucket;
import io.crate.core.collections.RowN;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.jobs.ExecutionState;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.operation.Input;
import io.crate.operation.RowDownstream;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.CollectingRowReceiver;
import io.crate.types.DataTypes;
import io.crate.types.StringType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.junit.Test;

import java.util.Arrays;
import java.util.UUID;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class IndexWriterProjectorTest extends SQLTransportIntegrationTest {

    private static final ColumnIdent ID_IDENT = new ColumnIdent("id");

    private static final TableIdent bulkImportIdent = new TableIdent(null, "bulk_import");

    @Test
    public void testIndexWriter() throws Throwable {
        execute("create table bulk_import (id int primary key, name string) with (number_of_replicas=0)");
        ensureGreen();

        CollectingRowReceiver collectingRowReceiver = new CollectingRowReceiver();
        InputCollectExpression sourceInput = new InputCollectExpression(1);
        InputColumn sourceInputColumn = new InputColumn(1, StringType.INSTANCE);
        CollectExpression[] collectExpressions = new CollectExpression[]{sourceInput};

        IndexWriterProjector writerProjector = new IndexWriterProjector(
                internalCluster().getInstance(ClusterService.class),
                ImmutableSettings.EMPTY,
                internalCluster().getInstance(TransportActionProvider.class),
                internalCluster().getInstance(BulkRetryCoordinatorPool.class),
                new TableIdent(null, "bulk_import"),
                null,
                new Reference(new ReferenceInfo(new ReferenceIdent(bulkImportIdent, DocSysColumns.RAW), RowGranularity.DOC, DataTypes.STRING)),
                Arrays.asList(ID_IDENT),
                Arrays.<Symbol>asList(new InputColumn(0)),
                Arrays.<Input<?>>asList(),
                null,
                null,
                sourceInput,
                sourceInputColumn,
                collectExpressions,
                20,
                null,
                null,
                false,
                false,
                UUID.randomUUID()
        );
        writerProjector.downstream(collectingRowReceiver);
        final RowDownstream rowDownstream = new ForwardingRowDownstream(writerProjector);

        final RowReceiver receiver1 = rowDownstream.newRowReceiver();
        receiver1.prepare(mock(ExecutionState.class));

        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 100; i++) {
                    receiver1.setNextRow(
                            new RowN(new Object[]{i, new BytesRef("{\"id\": " + i + ", \"name\": \"Arthur\"}")}));
                }
            }
        });


        final RowReceiver receiver2 = rowDownstream.newRowReceiver();
        receiver2.prepare(mock(ExecutionState.class));
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 100; i < 200; i++) {
                    receiver2.setNextRow(
                            new RowN(new Object[]{i, new BytesRef("{\"id\": " + i + ", \"name\": \"Trillian\"}")}));
                }
            }
        });
        t1.start();
        t2.start();

        t1.join();
        t2.join();
        receiver1.finish();
        receiver2.finish();
        Bucket objects = collectingRowReceiver.result();

        assertThat(objects, contains(isRow(200L)));

        execute("refresh table bulk_import");
        execute("select count(*) from bulk_import");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(200L));
    }
}
