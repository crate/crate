/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * A request to get indices level stats. Allow to enable different stats to be returned.
 * <p>
 * By default, the {@link #setDocs(boolean)} and {@link #setStore(boolean)}
 * are enabled. Other stats can be enabled as well.
 * <p>
 * All the stats to be returned can be cleared using {@link #clear()}, at which point, specific
 * stats can be enabled.
 */
public class IndicesStatsRequestBuilder extends BroadcastOperationRequestBuilder<IndicesStatsRequest, IndicesStatsResponse, IndicesStatsRequestBuilder> {

    public IndicesStatsRequestBuilder(ElasticsearchClient client, IndicesStatsAction action) {
        super(client, action, new IndicesStatsRequest());
    }

    /**
     * Sets all flags to return all stats.
     */
    public IndicesStatsRequestBuilder all() {
        request.all();
        return this;
    }

    /**
     * Clears all stats.
     */
    public IndicesStatsRequestBuilder clear() {
        request.clear();
        return this;
    }

    public IndicesStatsRequestBuilder setGroups(String... groups) {
        request.groups(groups);
        return this;
    }

    public IndicesStatsRequestBuilder setDocs(boolean docs) {
        request.docs(docs);
        return this;
    }

    public IndicesStatsRequestBuilder setStore(boolean store) {
        request.store(store);
        return this;
    }

    public IndicesStatsRequestBuilder setCompletion(boolean completion) {
        request.completion(completion);
        return this;
    }
}
