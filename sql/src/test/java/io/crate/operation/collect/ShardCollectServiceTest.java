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

package io.crate.operation.collect;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.WhereClause;
import io.crate.breaker.RamAccountingContext;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.projection.Projection;
import io.crate.test.integration.CrateIntegrationTest;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.UUID;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

@CrateIntegrationTest.ClusterScope(scope= CrateIntegrationTest.Scope.SUITE, numNodes = 1)
public class ShardCollectServiceTest extends SQLTransportIntegrationTest {

    public static final String TEST_TABLE_NAME = "ttt";
    private ShardCollectService shardCollectService;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(ShardCollectService.EXPIRATION_SETTING, ShardCollectService.EXPIRATION_DEFAULT)
                .build();
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void configure() {
        execute(String.format("create table %s (id int) with (number_of_replicas=0)", TEST_TABLE_NAME));
        ensureGreen();
        IndicesService indicesService = cluster().getInstance(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(TEST_TABLE_NAME);
        Injector injector = indexService.shardInjectorSafe(0);
        shardCollectService = injector.getInstance(ShardCollectService.class);
    }

    @After
    public void cleanup() {
        if (shardCollectService != null) {
            try {
                shardCollectService.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            shardCollectService = null;
        }
    }

    @Test
    public void testLuceneCollectContext() throws Throwable {

        TableInfo docTableInfo = cluster().getInstance(ReferenceInfos.class).getTableInfo(new TableIdent(ReferenceInfos.DEFAULT_SCHEMA_NAME, TEST_TABLE_NAME));
        CollectNode collectNode = new CollectNode("bla", docTableInfo.getRouting(WhereClause.MATCH_ALL, null));
        collectNode.jobId(UUID.randomUUID());
        collectNode.maxRowGranularity(RowGranularity.DOC);
        CrateCollector collector1 = shardCollectService.getCollector(collectNode, new ShardProjectorChain(1, ImmutableList.<Projection>of(), mock(ProjectionToProjectorVisitor.class), mock(RamAccountingContext.class)));

        assertThat(collector1, instanceOf(LuceneDocCollector.class));
        LuceneDocCollector luceneCollector1 = (LuceneDocCollector)collector1;
        Field privateContextField1 = LuceneDocCollector.class.getDeclaredField("shardCollectContext");
        privateContextField1.setAccessible(true);
        ShardCollectContext context1 = (ShardCollectContext)privateContextField1.get(luceneCollector1);

        CrateCollector collector2 = shardCollectService.getCollector(collectNode, new ShardProjectorChain(1, ImmutableList.<Projection>of(), mock(ProjectionToProjectorVisitor.class), mock(RamAccountingContext.class)));
        assertThat(collector2, instanceOf(LuceneDocCollector.class));
        LuceneDocCollector luceneCollector2 = (LuceneDocCollector)collector2;
        Field privateContextField2 = LuceneDocCollector.class.getDeclaredField("shardCollectContext");
        privateContextField2.setAccessible(true);
        ShardCollectContext context2 = (ShardCollectContext)privateContextField2.get(luceneCollector2);

        assertThat(context1, is(context2));

        collectNode.jobId(UUID.randomUUID());
        CrateCollector collector3 = shardCollectService.getCollector(collectNode, new ShardProjectorChain(1, ImmutableList.<Projection>of(), mock(ProjectionToProjectorVisitor.class), mock(RamAccountingContext.class)));
        assertThat(collector3, instanceOf(LuceneDocCollector.class));
        LuceneDocCollector luceneCollector3 = (LuceneDocCollector)collector3;
        Field privateContextField3= LuceneDocCollector.class.getDeclaredField("shardCollectContext");
        privateContextField3.setAccessible(true);
        ShardCollectContext context3 = (ShardCollectContext)privateContextField3.get(luceneCollector3);

        assertThat(context1, is(not(context3)));
    }
}
