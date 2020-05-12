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

package org.elasticsearch.action.admin.cluster.configuration;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class AddVotingConfigExclusionsAction extends Action<AddVotingConfigExclusionsRequest, AddVotingConfigExclusionsResponse, AddVotingConfigExclusionsAction.AddVotingConfigExclusionsRequestBuilder> {
    public static final AddVotingConfigExclusionsAction INSTANCE = new AddVotingConfigExclusionsAction();
    public static final String NAME = "cluster:admin/voting_config/add_exclusions";

    private AddVotingConfigExclusionsAction() {
        super(NAME);
    }

    @Override
    public AddVotingConfigExclusionsRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new AddVotingConfigExclusionsRequestBuilder(client, this, new AddVotingConfigExclusionsRequest(new String[0]));
    }

    static class AddVotingConfigExclusionsRequestBuilder extends ActionRequestBuilder<AddVotingConfigExclusionsRequest, AddVotingConfigExclusionsResponse, AddVotingConfigExclusionsRequestBuilder> {

        AddVotingConfigExclusionsRequestBuilder(ElasticsearchClient client,
                                                Action<AddVotingConfigExclusionsRequest, AddVotingConfigExclusionsResponse, AddVotingConfigExclusionsRequestBuilder> action,
                                                AddVotingConfigExclusionsRequest request) {
            super(client, action, request);
        }
    }
}
