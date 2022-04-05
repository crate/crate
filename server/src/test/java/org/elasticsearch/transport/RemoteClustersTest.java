/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.protocols.postgres.PgClientFactory;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteClustersTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_connect_reconnects_if_previous_call_raised_an_exception() {
        var pgClientFactory = mock(PgClientFactory.class);
        when(pgClientFactory.createClient(anyString(), any(ConnectionInfo.class))).thenThrow(new RuntimeException("dummy"));

        var remoteClusters = new RemoteClusters(
            Settings.EMPTY,
            THREAD_POOL,
            pgClientFactory,
            MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, THREAD_POOL)
        );

        var future = remoteClusters.connect("foo", ConnectionInfo.fromURL("crate://localhost?mode=pg_tunnel"));
        assertThat(future.isCompletedExceptionally(), is(true));

        // second call must also result in a `pgFactory.createClient()` call and such throw an exception
        future = remoteClusters.connect("foo", ConnectionInfo.fromURL("crate://localhost?mode=pg_tunnel"));
        assertThat(future.isCompletedExceptionally(), is(true));
    }
}
