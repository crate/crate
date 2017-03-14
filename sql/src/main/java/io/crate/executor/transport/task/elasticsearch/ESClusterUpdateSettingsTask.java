/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import io.crate.data.BatchConsumer;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.executor.JobTask;
import io.crate.executor.transport.OneRowActionListener;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.settings.SettingsApplier;
import io.crate.planner.node.ddl.ESClusterUpdateSettingsPlan;
import io.crate.sql.tree.Expression;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Map;

public class ESClusterUpdateSettingsTask extends JobTask {

    private static final Function<Object, Row> TO_ONE_ROW = Functions.<Row>constant(new Row1(1L));

    private final ESClusterUpdateSettingsPlan plan;
    private final TransportClusterUpdateSettingsAction transport;

    public ESClusterUpdateSettingsTask(ESClusterUpdateSettingsPlan plan,
                                       TransportClusterUpdateSettingsAction transport) {
        super(plan.jobId());
        this.plan = plan;
        this.transport = transport;
    }

    @Override
    public void execute(BatchConsumer consumer, Row parameters) {
        ClusterUpdateSettingsRequest request = buildESUpdateClusterSettingRequest(
            buildSettingsFrom(plan.persistentSettings(), parameters),
            buildSettingsFrom(plan.transientSettings(), parameters)
        );
        OneRowActionListener<ClusterUpdateSettingsResponse> actionListener = new OneRowActionListener<>(consumer, TO_ONE_ROW);
        transport.execute(request, actionListener);
    }

    static Settings buildSettingsFrom(Map<String, List<Expression>> settingsMap, Row parameters) {
        Settings.Builder settings = Settings.builder();
        for (Map.Entry<String, List<Expression>> entry : settingsMap.entrySet()) {
            String settingsName = entry.getKey();
            SettingsApplier settingsApplier = CrateSettings.getSettingsApplier(settingsName);
            settingsApplier.apply(settings, parameters, Iterables.getOnlyElement(entry.getValue()));
        }
        return settings.build();
    }

    private ClusterUpdateSettingsRequest buildESUpdateClusterSettingRequest(Settings persistentSettings,
                                                                            Settings transientSettings) {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(persistentSettings);
        request.transientSettings(transientSettings);
        if (plan.persistentSettingsToRemove() != null) {
            request.persistentSettingsToRemove(plan.persistentSettingsToRemove());
        }
        if (plan.transientSettingsToRemove() != null) {
            request.transientSettingsToRemove(plan.transientSettingsToRemove());
        }
        return request;
    }
}
