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

package io.crate.es.action.admin.indices.settings.put;

import io.crate.es.action.support.IndicesOptions;
import io.crate.es.action.support.master.AcknowledgedRequestBuilder;
import io.crate.es.action.support.master.AcknowledgedResponse;
import io.crate.es.client.ElasticsearchClient;
import io.crate.es.common.settings.Settings;
import io.crate.es.common.xcontent.XContentType;

import java.util.Map;

/**
 * Builder for an update index settings request
 */
public class UpdateSettingsRequestBuilder extends AcknowledgedRequestBuilder<UpdateSettingsRequest, AcknowledgedResponse, UpdateSettingsRequestBuilder> {

    public UpdateSettingsRequestBuilder(ElasticsearchClient client, UpdateSettingsAction action, String... indices) {
        super(client, action, new UpdateSettingsRequest(indices));
    }

    /**
     * Sets the indices the update settings will execute on
     */
    public UpdateSettingsRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     * <p>
     * For example indices that don't exist.
     */
    public UpdateSettingsRequestBuilder setIndicesOptions(IndicesOptions options) {
        request.indicesOptions(options);
        return this;
    }

    /**
     * Sets the settings to be updated
     */
    public UpdateSettingsRequestBuilder setSettings(Settings settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Sets the settings to be updated
     */
    public UpdateSettingsRequestBuilder setSettings(Settings.Builder settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Sets the settings to be updated (either json or yaml format)
     */
    public UpdateSettingsRequestBuilder setSettings(String source, XContentType xContentType) {
        request.settings(source, xContentType);
        return this;
    }

    /**
     * Sets the settings to be updated
     */
    public UpdateSettingsRequestBuilder setSettings(Map<String, Object> source) {
        request.settings(source);
        return this;
    }

    public UpdateSettingsRequestBuilder setPreserveExisting(boolean preserveExisting) {
        request.setPreserveExisting(preserveExisting);
        return this;
    }
}
