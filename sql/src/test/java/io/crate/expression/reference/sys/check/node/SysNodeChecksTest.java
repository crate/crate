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

package io.crate.expression.reference.sys.check.node;

import io.crate.expression.reference.sys.check.SysCheck;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.node.NodeService;
import org.junit.Test;
import org.mockito.Answers;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SysNodeChecksTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testRecoveryExpectedNodesCheckWithDefaultSetting() {
        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck =
            new RecoveryExpectedNodesSysCheck(clusterService, Settings.EMPTY);

        assertThat(recoveryExpectedNodesCheck.id(), is(1));
        assertThat(recoveryExpectedNodesCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(recoveryExpectedNodesCheck.validate(1,
            GatewayService.EXPECTED_NODES_SETTING.getDefault(Settings.EMPTY)), is(true));
    }

    @Test
    public void testRecoveryExpectedNodesCheckWithLessThanQuorum() {
        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck =
            new RecoveryExpectedNodesSysCheck(clusterService, Settings.EMPTY);

        assertThat(recoveryExpectedNodesCheck.id(), is(1));
        assertThat(recoveryExpectedNodesCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(recoveryExpectedNodesCheck.validate(2, 1), is(false));
    }

    @Test
    public void testRecoveryExpectedNodesCheckWithCorrectSetting() {
        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck =
            new RecoveryExpectedNodesSysCheck(clusterService, Settings.EMPTY);

        assertThat(recoveryExpectedNodesCheck.id(), is(1));
        assertThat(recoveryExpectedNodesCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(recoveryExpectedNodesCheck.validate(3, 3), is(true));
    }

    @Test
    public void testRecoveryExpectedNodesCheckWithBiggerThanNumberOfNodes() {
        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck =
            new RecoveryExpectedNodesSysCheck(clusterService, Settings.EMPTY);

        assertThat(recoveryExpectedNodesCheck.id(), is(1));
        assertThat(recoveryExpectedNodesCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(recoveryExpectedNodesCheck.validate(3, 4), is(false));
    }

    @Test
    public void testRecoveryAfterNodesCheckWithDefaultSetting() {
        RecoveryAfterNodesSysCheck recoveryAfterNodesCheck =
            new RecoveryAfterNodesSysCheck(clusterService, Settings.EMPTY);

        assertThat(recoveryAfterNodesCheck.id(), is(2));
        assertThat(recoveryAfterNodesCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(recoveryAfterNodesCheck.validate(
            GatewayService.RECOVER_AFTER_NODES_SETTING.getDefault(Settings.EMPTY),
            GatewayService.EXPECTED_NODES_SETTING.getDefault(Settings.EMPTY)
        ), is(true));
    }

    @Test
    public void testRecoveryAfterNodesCheckWithLessThanQuorum() {
        ClusterService clusterService = mock(ClusterService.class, Answers.RETURNS_DEEP_STUBS.get());

        RecoveryAfterNodesSysCheck recoveryAfterNodesCheck =
            new RecoveryAfterNodesSysCheck(clusterService, Settings.EMPTY);

        when(clusterService.localNode().getId()).thenReturn("node");
        when(clusterService.state().nodes().getSize()).thenReturn(8);

        assertThat(recoveryAfterNodesCheck.id(), is(2));
        assertThat(recoveryAfterNodesCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(recoveryAfterNodesCheck.validate(4, 8), is(false));
    }

    @Test
    public void testRecoveryAfterNodesCheckWithCorrectSetting() {
        ClusterService clusterService = mock(ClusterService.class, Answers.RETURNS_DEEP_STUBS.get());

        RecoveryAfterNodesSysCheck recoveryAfterNodesCheck =
            new RecoveryAfterNodesSysCheck(clusterService, Settings.EMPTY);

        when(clusterService.localNode().getId()).thenReturn("node");
        when(clusterService.state().nodes().getSize()).thenReturn(8);

        assertThat(recoveryAfterNodesCheck.id(), is(2));
        assertThat(recoveryAfterNodesCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(recoveryAfterNodesCheck.validate(8, 8), is(true));
    }

    @Test
    public void testRecoveryAfterTimeCheckWithCorrectSetting() {
        Settings settings = Settings.builder()
            .put(GatewayService.RECOVER_AFTER_TIME_SETTING.getKey(), TimeValue.timeValueMillis(4).toString())
            .put(GatewayService.RECOVER_AFTER_NODES_SETTING.getKey(), 3)
            .put(GatewayService.EXPECTED_NODES_SETTING.getKey(), 3)
            .build();

        RecoveryAfterTimeSysCheck recoveryAfterNodesCheck = new RecoveryAfterTimeSysCheck(clusterService, settings);
        assertThat(recoveryAfterNodesCheck.validate(), is(true));
    }

    @Test
    public void testRecoveryAfterTimeCheckWithDefaultSetting() {
        RecoveryAfterTimeSysCheck recoveryAfterNodesCheck =
            new RecoveryAfterTimeSysCheck(clusterService, Settings.EMPTY);

        assertThat(recoveryAfterNodesCheck.id(), is(3));
        assertThat(recoveryAfterNodesCheck.severity(), is(SysCheck.Severity.MEDIUM));
        assertThat(recoveryAfterNodesCheck.validate(
            GatewayService.RECOVER_AFTER_TIME_SETTING.getDefault(Settings.EMPTY),
            GatewayService.RECOVER_AFTER_NODES_SETTING.getDefault(Settings.EMPTY),
            GatewayService.EXPECTED_NODES_SETTING.getDefault(Settings.EMPTY)
        ), is(true));
    }

    @Test
    public void testRecoveryAfterTimeCheckWithWrongSetting() {
        Settings settings = Settings.builder()
            .put(GatewayService.RECOVER_AFTER_TIME_SETTING.getKey(), TimeValue.timeValueMillis(0).toString())
            .put(GatewayService.RECOVER_AFTER_NODES_SETTING.getKey(), 3)
            .put(GatewayService.EXPECTED_NODES_SETTING.getKey(), 3)
            .build();

        RecoveryAfterTimeSysCheck recoveryAfterNodesCheck = new RecoveryAfterTimeSysCheck(clusterService, settings);

        assertThat(recoveryAfterNodesCheck.validate(), is(false));
    }

    @Test
    public void testValidationLowDiskWatermarkCheck() {
        DiskWatermarkNodesSysCheck low = new LowDiskWatermarkNodesSysCheck(
            clusterService,
            Settings.EMPTY,
            mock(NodeService.class, Answers.RETURNS_MOCKS.get())
        );

        assertThat(low.id(), is(6));
        assertThat(low.severity(), is(SysCheck.Severity.HIGH));

        // default threshold is: 85% used
        assertThat(low.validate(15, 100), is(true));
        assertThat(low.validate(14, 100), is(false));
    }

    @Test
    public void testValidationHighDiskWatermarkCheck() {
        DiskWatermarkNodesSysCheck high = new HighDiskWatermarkNodesSysCheck(
            clusterService,
            Settings.EMPTY,
            mock(NodeService.class, Answers.RETURNS_MOCKS.get())
        );

        assertThat(high.id(), is(5));
        assertThat(high.severity(), is(SysCheck.Severity.HIGH));

        // default threshold is: 90% used
        assertThat(high.validate(10, 100), is(true));
        assertThat(high.validate(9, 100), is(false));
    }

    @Test
    public void testValidationFloodStageDiskWatermarkCheck() {
        DiskWatermarkNodesSysCheck floodStage = new FloodStageDiskWatermarkNodesSysCheck(
            clusterService,
            Settings.EMPTY,
            mock(NodeService.class, Answers.RETURNS_MOCKS.get())
        );

        assertThat(floodStage.id(), is(7));
        assertThat(floodStage.severity(), is(SysCheck.Severity.HIGH));

        // default threshold is: 95% used
        assertThat(floodStage.validate(5, 100), is(true));
        assertThat(floodStage.validate(4, 100), is(false));
    }
}
