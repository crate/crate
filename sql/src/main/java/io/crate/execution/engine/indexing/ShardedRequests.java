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

package io.crate.execution.engine.indexing;

import io.crate.execution.dml.ShardRequest;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class ShardedRequests<TReq extends ShardRequest<TReq, TItem>, TItem extends ShardRequest.Item> {

    final Map<String, List<ItemAndRoutingAndSourceInfo<TItem>>> itemsByMissingIndex = new HashMap<>();
    final Map<BytesRef, List<ReadFailureAndLineNumber>> itemsWithFailureBySourceUri = new HashMap<>();
    final Map<BytesRef, String> sourceUrisWithFailure = new HashMap<>();
    final List<RowSourceInfo> rowSourceInfos = new ArrayList<>();
    final Map<ShardLocation, TReq> itemsByShard = new HashMap<>();

    private final Function<ShardId, TReq> requestFactory;

    private int location = -1;

    /**
     * @param requestFactory function to create a request
     */
    ShardedRequests(Function<ShardId, TReq> requestFactory) {
        this.requestFactory = requestFactory;
    }

    public void add(TItem item, String indexName, String routing, RowSourceInfo rowSourceInfo) {
        List<ItemAndRoutingAndSourceInfo<TItem>> items = itemsByMissingIndex.computeIfAbsent(indexName, k -> new ArrayList<>());
        items.add(new ItemAndRoutingAndSourceInfo<>(item, routing, rowSourceInfo));
    }

    public void add(TItem item, ShardLocation shardLocation, RowSourceInfo rowSourceInfo) {
        TReq req = itemsByShard.get(shardLocation);
        if (req == null) {
            req = requestFactory.apply(shardLocation.shardId);
            itemsByShard.put(shardLocation, req);
        }
        location++;
        req.add(location, item);
        rowSourceInfos.add(rowSourceInfo);
    }

    void addFailedItem(BytesRef sourceUri, String readFailure, Long lineNumber) {
        List<ReadFailureAndLineNumber> itemsWithFailure = itemsWithFailureBySourceUri.computeIfAbsent(
            sourceUri, k -> new ArrayList<>());
        itemsWithFailure.add(new ReadFailureAndLineNumber(readFailure, lineNumber));
    }

    void addFailedUri(BytesRef sourceUri, String uriReadFailure) {
        assert sourceUrisWithFailure.get(sourceUri) == null : "A failure was already stored for this URI, should happen only once";
        sourceUrisWithFailure.put(sourceUri, uriReadFailure);
    }

    static class ItemAndRoutingAndSourceInfo<TItem> {
        final TItem item;
        final String routing;
        final RowSourceInfo rowSourceInfo;

        ItemAndRoutingAndSourceInfo(TItem item, String routing, RowSourceInfo rowSourceInfo) {
            this.item = item;
            this.routing = routing;
            this.rowSourceInfo = rowSourceInfo;
        }
    }

    static class ReadFailureAndLineNumber {
        final String readFailure;
        final long lineNumber;

        ReadFailureAndLineNumber(String readFailure, long lineNumber) {
            this.readFailure = readFailure;
            this.lineNumber = lineNumber;
        }
    }
}
