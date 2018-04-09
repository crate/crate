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

package io.crate.execution.ddl.tables;

import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.planner.node.ddl.CreateAnalyzerPlan;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;

import java.util.function.Function;

public class CreateAnalyzerTask {

    private static final Function<Object, Row> TO_ONE_ROW = o -> new Row1(1L);

    private final CreateAnalyzerPlan plan;
    private final TransportClusterUpdateSettingsAction transport;

    public CreateAnalyzerTask(CreateAnalyzerPlan plan,
                              TransportClusterUpdateSettingsAction transport) {
        this.plan = plan;
        this.transport = transport;
    }

    public void execute(RowConsumer consumer) {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(plan.createAnalyzerSettings());

        OneRowActionListener<ClusterUpdateSettingsResponse> actionListener =
            new OneRowActionListener<>(consumer, TO_ONE_ROW);
        transport.execute(request, actionListener);
    }
}
