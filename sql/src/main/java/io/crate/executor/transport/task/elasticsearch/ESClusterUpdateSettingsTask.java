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
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.action.sql.ResultReceiver;
import io.crate.core.collections.Row;
import io.crate.core.collections.Row1;
import io.crate.executor.JobTask;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.OneRowActionListener;
import io.crate.planner.node.ddl.ESClusterUpdateSettingsPlan;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;

import java.util.List;

public class ESClusterUpdateSettingsTask extends JobTask {

    private static final Function<Object, Row> TO_ONE_ROW = Functions.<Row>constant(new Row1(1L));;
    private final TransportClusterUpdateSettingsAction transport;
    private final ClusterUpdateSettingsRequest request;

    public ESClusterUpdateSettingsTask(ESClusterUpdateSettingsPlan plan,
                                       TransportClusterUpdateSettingsAction transport) {
        super(plan.jobId());
        this.transport = transport;

        request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(plan.persistentSettings());
        request.transientSettings(plan.transientSettings());
        if (plan.persistentSettingsToRemove() != null) {
            request.persistentSettingsToRemove(plan.persistentSettingsToRemove());
        }
        if (plan.transientSettingsToRemove() != null) {
            request.transientSettingsToRemove(plan.transientSettingsToRemove());
        }
    }

    @Override
    public void execute(ResultReceiver resultReceiver) {
        OneRowActionListener<ClusterUpdateSettingsResponse> actionListener = new OneRowActionListener<>(resultReceiver, TO_ONE_ROW);
        transport.execute(request, actionListener);
    }

    @Override
    public List<? extends ListenableFuture<TaskResult>> executeBulk() {
        throw new UnsupportedOperationException("cluster update settings task cannot be executed as bulk operation");
    }
}
