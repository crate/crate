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

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;

public class SimulatePipelineRequestBuilder extends ActionRequestBuilder<SimulatePipelineRequest, SimulatePipelineResponse, SimulatePipelineRequestBuilder> {

    /**
     * Create a new builder for {@link SimulatePipelineRequest}s
     */
    public SimulatePipelineRequestBuilder(ElasticsearchClient client, SimulatePipelineAction action) {
        super(client, action, new SimulatePipelineRequest());
    }

    /**
     * Create a new builder for {@link SimulatePipelineRequest}s
     * @deprecated use {@link #SimulatePipelineRequestBuilder(ElasticsearchClient, SimulatePipelineAction, BytesReference, XContentType)} to
     *             avoid content type auto-detection on the source bytes
     */
    @Deprecated
    public SimulatePipelineRequestBuilder(ElasticsearchClient client, SimulatePipelineAction action, BytesReference source) {
        super(client, action, new SimulatePipelineRequest(source));
    }

    /**
     * Create a new builder for {@link SimulatePipelineRequest}s
     */
    public SimulatePipelineRequestBuilder(ElasticsearchClient client, SimulatePipelineAction action, BytesReference source,
                                          XContentType xContentType) {
        super(client, action, new SimulatePipelineRequest(source, xContentType));
    }

    /**
     * Set the id for the pipeline to simulate
     */
    public SimulatePipelineRequestBuilder setId(String id) {
        request.setId(id);
        return this;
    }

    /**
     * Enable or disable verbose mode
     */
    public SimulatePipelineRequestBuilder setVerbose(boolean verbose) {
        request.setVerbose(verbose);
        return this;
    }

}
