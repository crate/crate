/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.cratedb.action.searchinto;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Container class for inout specific informations.
 */
public class SearchIntoContext extends SearchContext {

    // currently we only support index targets
    private String targetType = "index";

    private List<InetSocketTransportAddress> targetNodes;


    public Map<String, String> outputNames() {
        return outputNames;
    }

    private final Map<String, String> outputNames = new HashMap<String,
            String>();

    public SearchIntoContext(long id, ShardSearchRequest request,
            SearchShardTarget shardTarget, Engine.Searcher engineSearcher,
            IndexService indexService, IndexShard indexShard,
            ScriptService scriptService, CacheRecycler cacheRecycler) {
        super(id, request, shardTarget, engineSearcher, indexService,
                indexShard, scriptService, cacheRecycler);
    }

    public String targetType() {
        // this is currently the only type supported
        return targetType;
    }

    public List<InetSocketTransportAddress> targetNodes() {
        if (targetNodes == null) {
            targetNodes = Lists.newArrayList();
        }
        return targetNodes;
    }

    public void emptyTargetNodes() {
        this.targetNodes = ImmutableList.of();
    }

}
