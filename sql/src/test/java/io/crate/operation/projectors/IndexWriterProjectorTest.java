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
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.types.DataTypes;
import io.crate.types.StringType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.TransportShardUpsertActionDelegateImpl;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class IndexWriterProjectorTest extends SQLTransportIntegrationTest {

    private static final ColumnIdent ID_IDENT = new ColumnIdent("id");

    private static final TableIdent bulkImportIdent = new TableIdent(null, "bulk_import");

    @Test
    public void testIndexWriter() throws Throwable {
        execute("create table bulk_import (id int primary key, name string) with (number_of_replicas=0)");
        ensureGreen();

        CollectingProjector collectingProjector = new CollectingProjector();
        InputCollectExpression<Object> sourceInput = new InputCollectExpression<>(1);
        InputColumn sourceInputColumn = new InputColumn(1, StringType.INSTANCE);
        CollectExpression[] collectExpressions = new CollectExpression[]{sourceInput};

        ReferenceInfos referenceInfos = cluster().getInstance(ReferenceInfos.class);

        final IndexWriterProjector indexWriter = new IndexWriterProjector(
                cluster().getInstance(ClusterService.class),
                ImmutableSettings.EMPTY,
                cluster().getInstance(TransportShardUpsertActionDelegateImpl.class),
                cluster().getInstance(TransportCreateIndexAction.class),
                new TableIdent(null, "bulk_import"),
                null,
                new Reference(new ReferenceInfo(new ReferenceIdent(bulkImportIdent, DocSysColumns.RAW), RowGranularity.DOC, DataTypes.STRING)),
                Arrays.asList(ID_IDENT),
                Arrays.<Symbol>asList(new InputColumn(0)),
                Arrays.<Input<?>>asList(),
                null,
                sourceInput,
                sourceInputColumn,
                collectExpressions,
                20,
                null, null,
                false,
                false
        );
        indexWriter.registerUpstream(null);
        indexWriter.startProjection();
        indexWriter.downstream(collectingProjector);

        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 100; i++) {
                    indexWriter.setNextRow(
                            new RowN(new Object[]{i, new BytesRef("{\"id\": " + i + ", \"name\": \"Arthur\"}")}));
                }
            }
        });
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 100; i < 200; i++) {
                    indexWriter.setNextRow(
                            new RowN(new Object[]{i, new BytesRef("{\"id\": " + i + ", \"name\": \"Trillian\"}")}));
                }
            }
        });
        t1.start();
        t2.start();

        t1.join();
        t2.join();
        indexWriter.finish();
        Bucket objects = collectingProjector.result().get();

        assertThat(objects, contains(isRow(200L)));

        execute("refresh table bulk_import");
        execute("select count(*) from bulk_import");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(200L));
    }
}
