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

package io.crate.execution.support;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.es.action.ActionListener;
import io.crate.es.transport.ConnectTransportException;
import io.crate.es.transport.NodeNotConnectedException;
import io.crate.es.transport.TransportRequest;
import io.crate.es.transport.TransportResponseHandler;
import io.crate.es.transport.TransportService;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TransportsTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testOnFailureOnListenerIsCalledIfNodeIsNotInClusterState() throws Exception {
        Transports transports = new Transports(clusterService, mock(TransportService.class));
        ActionListener actionListener = mock(ActionListener.class);
        transports.sendRequest("actionName",
            "invalid", mock(TransportRequest.class), actionListener, mock(TransportResponseHandler.class));

        verify(actionListener, times(1)).onFailure(any(ConnectTransportException.class));
    }

    @Test
    public void testNodeNotConnectedDoesNotRequireDiscoveryNode() throws Exception {
        // must make sure ES upgrades don't break our code
        //noinspection ThrowableNotThrown
        new NodeNotConnectedException(null, "msg");
    }
}
