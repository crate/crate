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
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.ColumnIdent;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.test.integration.CrateIntegrationTest;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.client.Client;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class IndexWriterProjectorTest extends SQLTransportIntegrationTest {

    private static final ColumnIdent ID_IDENT = new ColumnIdent("id");

    @Test
    public void testIndexWriter() throws Throwable {
        execute("create table bulk_import (id int primary key, name string) with (number_of_replicas=0)");
        ensureGreen();

        CollectingProjector collectingProjector = new CollectingProjector();
        InputCollectExpression<Object> idInput = new InputCollectExpression<>(0);
        InputCollectExpression<Object> sourceInput = new InputCollectExpression<>(1);
        CollectExpression[] collectExpressions = new CollectExpression[]{ idInput, sourceInput };

        final IndexWriterProjector indexWriter = new IndexWriterProjector(
                cluster().getInstance(Client.class),
                "bulk_import",
                Arrays.asList(ID_IDENT),
                Arrays.<Input<?>>asList(idInput),
                ImmutableList.<Input<?>>of(),
                new ColumnIdent("id"),
                idInput,
                sourceInput,
                collectExpressions,
                20,
                2,
                null, null
        );
        indexWriter.registerUpstream(null);
        indexWriter.startProjection();
        indexWriter.downstream(collectingProjector);

        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 100; i++) {
                    indexWriter.setNextRow(i,
                            new BytesRef("{\"id\": " + i + ", \"name\": \"Arthur\"}"));
                }
            }
        });
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 100; i < 200; i++) {
                    indexWriter.setNextRow(i,
                            new BytesRef("{\"id\": " + i + ", \"name\": \"Trillian\"}"));
                }
            }
        });
        t1.start();
        t2.start();

        t1.join();
        t2.join();
        indexWriter.upstreamFinished();
        Object[][] objects = collectingProjector.result().get();
        assertThat((Long)objects[0][0], is(200L));

        execute("refresh table bulk_import");
        execute("select count(*) from bulk_import");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long)response.rows()[0][0], is(200L));
    }
}
