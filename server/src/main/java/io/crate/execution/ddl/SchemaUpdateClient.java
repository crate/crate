/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.execution.ddl;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.Mapping;

import io.crate.common.unit.TimeValue;

@Singleton
public class SchemaUpdateClient {

    public static final Setting<TimeValue> INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING =
        Setting.positiveTimeSetting("indices.mapping.dynamic_timeout", TimeValue.timeValueSeconds(30),
            Property.Dynamic, Property.NodeScope);

    private final TransportSchemaUpdateAction schemaUpdateAction;
    private volatile TimeValue dynamicMappingUpdateTimeout;

    @Inject
    public SchemaUpdateClient(Settings settings,
                              ClusterSettings clusterSettings,
                              TransportSchemaUpdateAction schemaUpdateAction) {
        this.schemaUpdateAction = schemaUpdateAction;
        this.dynamicMappingUpdateTimeout = SchemaUpdateClient.INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            SchemaUpdateClient.INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING, this::setDynamicMappingUpdateTimeout);
    }

    private void setDynamicMappingUpdateTimeout(TimeValue dynamicMappingUpdateTimeout) {
        this.dynamicMappingUpdateTimeout = dynamicMappingUpdateTimeout;
    }

    public void blockingUpdateOnMaster(Index index, Mapping mappingUpdate) {
        TimeValue timeout = this.dynamicMappingUpdateTimeout;
        SchemaUpdateRequest request = new SchemaUpdateRequest(index, mappingUpdate.toString());
        request.masterNodeTimeout(timeout);
        var response = FutureUtils.get(schemaUpdateAction.execute(request), timeout);
        if (!response.isAcknowledged()) {
            throw new ElasticsearchTimeoutException("Failed to acknowledge mapping update within [" + timeout + "]");
        }
    }
}
