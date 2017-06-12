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

package io.crate.operation.primarykey;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.crate.Constants;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.where.DocKeys;
import io.crate.data.Row;
import io.crate.executor.transport.task.elasticsearch.GetResponseRefResolver;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.operation.InputFactory;
import io.crate.operation.InputRow;
import io.crate.operation.collect.CollectExpression;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Singleton
public class PrimaryKeyLookupOperation {

    private final static Set<ColumnIdent> FETCH_SOURCE_COLUMNS = ImmutableSet.of(DocSysColumns.DOC, DocSysColumns.RAW);
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final Functions functions;

    @Inject
    public PrimaryKeyLookupOperation(
            ClusterService clusterService,
            Functions functions,
            IndicesService indicesService) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.functions = functions;
    }

    public CompletableFuture<Iterable<Row>> primaryKeyLookup(
            Map<ShardId, List<DocKeys.DocKey>> docKeysPerShard,
            Map<ColumnIdent, Integer> pkMapping,
            List<Symbol> toCollect) throws IOException, InterruptedException {

        MetaData metaData = clusterService.state().getMetaData();
        InputFactory inputFactory = new InputFactory(functions);
        Map<String, DocKeys.DocKey> docKeysById = groupDocKeysById(docKeysPerShard);
        List<ColumnIdent> columns = new ArrayList<>();
        GetResponseRefResolver refResolver = new GetResponseRefResolver(columns::add, pkMapping, docKeysById);
        InputFactory.Context<CollectExpression<GetResponse, ?>> ctx = inputFactory.ctxForRefs(refResolver);
        ctx.add(toCollect);

        List<CollectExpression<GetResponse, ?>> expressions = ctx.expressions();
        List<GetResponse> responses = new ArrayList<>();

        for (Map.Entry<ShardId, List<DocKeys.DocKey>> docKeysAtShard : docKeysPerShard.entrySet()) {
            ShardId shardId = docKeysAtShard.getKey();

            IndexMetaData indexMetaData = metaData.index(shardId.getIndex());
            Index index2 = indexMetaData.getIndex();
            IndexService indexService = indicesService.indexServiceSafe(index2);
            IndexShard shard = indexService.getShard(shardId.id());

            for (DocKeys.DocKey docKey : docKeysAtShard.getValue()) {
                Long version = docKey.version().orElse(Versions.MATCH_ANY);
                GetResult result = shard.getService().get(
                    Constants.DEFAULT_MAPPING_TYPE,
                    docKey.id(),
                    null,
                    // configure realtime access (= looks through the translog)
                    true,
                    version,
                    VersionType.INTERNAL,
                    getFetchSourceContext(columns)
                );

                responses.add(new GetResponse(result));
            }
        }

        responses = responses.stream()
            .filter(GetResponse::isExists)
            .collect(Collectors.toList());

        InputRow inputRow = new InputRow(ctx.topLevelInputs());
        List<Row> rows = Lists.transform(responses, r -> {
            for (CollectExpression<GetResponse, ?> expression : expressions) {
                expression.setNextRow(r);
            }
            return inputRow;
        });

        return CompletableFuture.completedFuture(rows);
    }

    private static Map<String, DocKeys.DocKey> groupDocKeysById(
            Map<ShardId, List<DocKeys.DocKey>> docKeysPerShard)
    {
        Map<String, DocKeys.DocKey> keysById = new HashMap<>();
        for (List<DocKeys.DocKey> docKeyList : docKeysPerShard.values()) {
            for (DocKeys.DocKey key : docKeyList) {
                keysById.put(key.id(), key);
            }
        }
        return keysById;
    }

    private static FetchSourceContext getFetchSourceContext(List<ColumnIdent> columns) {
        List<String> includes = new ArrayList<>(columns.size());
        for (ColumnIdent col : columns) {
            if (col.isSystemColumn() && FETCH_SOURCE_COLUMNS.contains(col)) {
                return new FetchSourceContext(true);
            }
            includes.add(col.name());
        }
        if (includes.size() > 0) {
            return new FetchSourceContext(includes.toArray(new String[includes.size()]));
        }
        return new FetchSourceContext(false);
    }


}
