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

package org.elasticsearch.action.admin.indices.upgrade.get;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class UpgradeStatusAction extends Action<UpgradeStatusRequest, UpgradeStatusResponse, UpgradeStatusRequestBuilder> {

    public static final UpgradeStatusAction INSTANCE = new UpgradeStatusAction();
    public static final String NAME = "indices:monitor/upgrade";

    private UpgradeStatusAction() {
        super(NAME);
    }

    @Override
    public UpgradeStatusResponse newResponse() {
        return new UpgradeStatusResponse();
    }

    @Override
    public UpgradeStatusRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new UpgradeStatusRequestBuilder(client, this);
    }
}
