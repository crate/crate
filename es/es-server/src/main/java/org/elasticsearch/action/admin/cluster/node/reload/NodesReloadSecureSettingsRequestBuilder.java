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

package org.elasticsearch.action.admin.cluster.node.reload;

import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Builder for the reload secure settings nodes request
 */
public class NodesReloadSecureSettingsRequestBuilder extends NodesOperationRequestBuilder<NodesReloadSecureSettingsRequest,
        NodesReloadSecureSettingsResponse, NodesReloadSecureSettingsRequestBuilder> {

    public NodesReloadSecureSettingsRequestBuilder(ElasticsearchClient client, NodesReloadSecureSettingsAction action) {
        super(client, action, new NodesReloadSecureSettingsRequest());
    }

}
