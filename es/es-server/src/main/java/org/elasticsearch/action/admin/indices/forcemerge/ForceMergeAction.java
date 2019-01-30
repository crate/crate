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

package org.elasticsearch.action.admin.indices.forcemerge;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class ForceMergeAction extends Action<ForceMergeRequest, ForceMergeResponse, ForceMergeRequestBuilder> {

    public static final ForceMergeAction INSTANCE = new ForceMergeAction();
    public static final String NAME = "indices:admin/forcemerge";

    private ForceMergeAction() {
        super(NAME);
    }

    @Override
    public ForceMergeResponse newResponse() {
        return new ForceMergeResponse();
    }

    @Override
    public ForceMergeRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new ForceMergeRequestBuilder(client, this);
    }
}
