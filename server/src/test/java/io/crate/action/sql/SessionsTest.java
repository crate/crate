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

package io.crate.action.sql;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

import io.crate.analyze.Analyzer;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.execution.jobs.transport.CancelRequest;
import io.crate.execution.jobs.transport.TransportCancelAction;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Planner;
import io.crate.protocols.postgres.KeyData;
import io.crate.user.User;
import io.crate.user.UserLookup;

public class SessionsTest {

    @Test
    public void test_sessions_broadcasts_cancel_if_no_local_match() throws Exception {
        Functions functions = new Functions(Map.of());
        UserLookup userLookup = () -> List.of(User.CRATE_USER);
        NodeContext nodeCtx = new NodeContext(functions, userLookup);
        DependencyCarrier dependencies = mock(DependencyCarrier.class);
        ElasticsearchClient client = mock(ElasticsearchClient.class, Answers.RETURNS_MOCKS);
        when(dependencies.client()).thenReturn(client);
        Sessions sessions = new Sessions(
            nodeCtx,
            mock(Analyzer.class),
            mock(Planner.class),
            () -> dependencies,
            new JobsLogs(() -> false),
            Settings.EMPTY,
            mock(ClusterService.class)
        );

        KeyData keyData = new KeyData(10, 20);
        sessions.cancel(keyData);
        verify(client).execute(
            Mockito.eq(TransportCancelAction.ACTION),
            Mockito.eq(new CancelRequest(keyData))
        );
    }
}
