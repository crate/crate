/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport;

import com.google.common.collect.Lists;
import io.crate.core.collections.RowN;
import io.crate.jobs.JobContextService;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ShortType;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class TransportShardUpsertActionTest extends CrateUnitTest {

    static class TestingTransportShardUpsertAction extends TransportShardUpsertAction {

        public TestingTransportShardUpsertAction(Settings settings,
                                                 ThreadPool threadPool,
                                                 ClusterService clusterService,
                                                 TransportService transportService,
                                                 ActionFilters actionFilters,
                                                 TransportIndexAction indexAction,
                                                 IndicesService indicesService,
                                                 JobContextService jobContextService,
                                                 ShardStateAction shardStateAction,
                                                 Functions functions) {
            super(settings, threadPool, clusterService, transportService, actionFilters,
                    indexAction, indicesService, jobContextService, shardStateAction, functions);
        }

        @Override
        protected IndexResponse indexItem(ShardUpsertRequest request,
                                          ShardUpsertRequest.Item item,
                                          ShardId shardId,
                                          SymbolToFieldExtractorContext extractorContextUpdate,
                                          SymbolToInputContext implContextInsert,
                                          boolean tryInsertFirst,
                                          int retryCount) throws ElasticsearchException {
            throw new IndexMissingException(new Index(request.index()));
        }
    }

    private TransportShardUpsertAction transportShardUpsertAction;

    @Before
    public void prepare() throws Exception {
        transportShardUpsertAction = new TestingTransportShardUpsertAction(
                ImmutableSettings.EMPTY,
                mock(ThreadPool.class),
                mock(ClusterService.class),
                mock(TransportService.class),
                mock(ActionFilters.class),
                mock(TransportIndexAction.class),
                mock(IndicesService.class),
                mock(JobContextService.class),
                mock(ShardStateAction.class),
                mock(Functions.class)
                );
    }

    @Test
    public void testIndexMissingExceptionWhileProcessingItemsResultsInFailure() throws Exception {
        TableIdent charactersIdent = new TableIdent(null, "characters");

        final Reference idRef = new Reference(new ReferenceInfo(
                new ReferenceIdent(charactersIdent, "id"), RowGranularity.DOC, DataTypes.SHORT));
        DataType[] dataTypes = new DataType[]{ ShortType.INSTANCE };
        Lists.newArrayList(0);
        List<Integer> rowIndicesToStream = Lists.newArrayList(0);
        Map<Reference, Symbol> insertAssignments = new HashMap<Reference, Symbol>(){{
            put(idRef, new InputColumn(0));
        }};

        ShardId shardId = new ShardId("characters", 0);
        ShardUpsertRequest request = new ShardUpsertRequest(
                shardId,
                dataTypes,
                rowIndicesToStream,
                UUID.randomUUID(),
                null,
                insertAssignments);
        request.add(1, "1", new RowN(new Object[]{1}), null, null);

        ShardUpsertResponse response = transportShardUpsertAction.processRequestItems(
                shardId,
                request,
                mock(TransportShardUpsertAction.SymbolToFieldExtractorContext.class),
                mock(TransportShardUpsertAction.SymbolToInputContext.class),
                new AtomicBoolean(false));

        assertThat(response.failures().size(), is(1));
        assertThat(response.failures().get(0).message(), is("IndexMissingException[[characters] missing]"));
    }
}
