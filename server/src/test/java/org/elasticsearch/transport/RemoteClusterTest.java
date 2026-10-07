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

package org.elasticsearch.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.protocols.postgres.PgClient;
import io.crate.protocols.postgres.PgClientFactory;
import io.crate.replication.logical.metadata.ConnectionInfo;

public class RemoteClusterTest extends ESTestCase {

    @Test
    public void test_connection_future_is_shared_across_calls() throws Exception {
        ConnectionInfo connectionInfo = ConnectionInfo.fromURL("crate://localhost?mode=pg_tunnel");
        PgClientFactory pgClientFactory = mock(PgClientFactory.class);
        PgClient pgClient = mock(PgClient.class);
        CompletableFuture<Transport.Connection> connectionFuture = new CompletableFuture<>();
        when(pgClientFactory.createClient("sub1", connectionInfo)).thenReturn(pgClient);
        when(pgClient.ensureConnected()).thenReturn(connectionFuture);

        try (RemoteCluster remoteCluster = new RemoteCluster(
            "sub1", Settings.EMPTY, connectionInfo, pgClientFactory, mock(TransportService.class))) {
            CompletableFuture<Client> first = remoteCluster.connectAndGetClient();
            CompletableFuture<Client> second = remoteCluster.connectAndGetClient();

            assertThat(second).isSameAs(first);
            connectionFuture.complete(mock(Transport.Connection.class));
            assertThat(remoteCluster.connectAndGetClient()).isSameAs(first);
        }
    }
}
