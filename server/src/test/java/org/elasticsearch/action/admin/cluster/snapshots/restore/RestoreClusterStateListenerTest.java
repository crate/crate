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

package org.elasticsearch.action.admin.cluster.snapshots.restore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.snapshots.RestoreService.RestoreCompletionResponse;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.concurrent.FutureActionListener;

public class RestoreClusterStateListenerTest extends ESTestCase {

    @Test
    public void test_restore_completed_before_registration_does_not_wait_for_another_event() {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.builder(ClusterName.DEFAULT).build());
        var response = mock(RestoreCompletionResponse.class);
        when(response.getUuid()).thenReturn("restore-1");
        var result = new FutureActionListener<RestoreSnapshotResponse>();
        var listener = new RestoreClusterStateListener(clusterService, response, result);

        listener.start();

        assertThat(result.isDone()).isTrue();
        assertThat(result.join().getRestoreInfo()).isNull();
        verify(clusterService).removeListener(listener);
    }
}
