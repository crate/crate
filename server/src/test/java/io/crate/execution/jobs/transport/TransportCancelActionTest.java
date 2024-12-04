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

package io.crate.execution.jobs.transport;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;
import org.mockito.Mockito;

import io.crate.session.Sessions;
import io.crate.protocols.postgres.KeyData;

public class TransportCancelActionTest {

    @Test
    public void test_node_op_calls_sessions_cancel_locally() throws Exception {
        var sessions = mock(Sessions.class);
        TransportCancelAction transportCancelAction = new TransportCancelAction(
            sessions,
            mock(ClusterService.class),
            mock(TransportService.class)
        );
        KeyData keyData = new KeyData(42, 21);
        transportCancelAction.nodeOperation(new CancelRequest(keyData));
        verify(sessions).cancelLocally(Mockito.eq(keyData));
    }
}
