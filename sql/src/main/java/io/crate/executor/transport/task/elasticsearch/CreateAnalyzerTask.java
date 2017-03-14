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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import io.crate.data.BatchConsumer;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.executor.JobTask;
import io.crate.executor.transport.OneRowActionListener;
import io.crate.planner.node.ddl.CreateAnalyzerPlan;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;

public class CreateAnalyzerTask extends JobTask {

    private static final Function<Object, Row> TO_ONE_ROW = Functions.<Row>constant(new Row1(1L));

    private final CreateAnalyzerPlan plan;
    private final TransportClusterUpdateSettingsAction transport;

    public CreateAnalyzerTask(CreateAnalyzerPlan plan,
                              TransportClusterUpdateSettingsAction transport) {
        super(plan.jobId());
        this.plan = plan;
        this.transport = transport;
    }

    @Override
    public void execute(BatchConsumer consumer, Row parameters) {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(plan.createAnalyzerSettings());

        OneRowActionListener<ClusterUpdateSettingsResponse> actionListener =
            new OneRowActionListener<>(consumer, TO_ONE_ROW);
        transport.execute(request, actionListener);
    }
}
