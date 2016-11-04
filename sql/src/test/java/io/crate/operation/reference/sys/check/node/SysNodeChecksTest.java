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

package io.crate.operation.reference.sys.check.node;

import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.reference.sys.check.SysCheck;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.monitor.MonitorService;
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
            CrateSettings.GATEWAY_EXPECTED_NODES.defaultValue()), is(true));
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
            CrateSettings.GATEWAY_RECOVERY_AFTER_NODES.defaultValue(),
            CrateSettings.GATEWAY_EXPECTED_NODES.defaultValue()
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
        RecoveryAfterTimeSysCheck recoveryAfterNodesCheck =
            new RecoveryAfterTimeSysCheck(clusterService, Settings.EMPTY);

        assertThat(recoveryAfterNodesCheck.id(), is(3));
        assertThat(recoveryAfterNodesCheck.severity(), is(SysCheck.Severity.MEDIUM));
        assertThat(recoveryAfterNodesCheck.validate(TimeValue.timeValueMinutes(6), 1, 4), is(true));
    }

    @Test
    public void testRecoveryAfterTimeCheckWithDefaultSetting() {
        RecoveryAfterTimeSysCheck recoveryAfterNodesCheck =
            new RecoveryAfterTimeSysCheck(clusterService, Settings.EMPTY);

        assertThat(recoveryAfterNodesCheck.id(), is(3));
        assertThat(recoveryAfterNodesCheck.severity(), is(SysCheck.Severity.MEDIUM));
        assertThat(recoveryAfterNodesCheck.validate(
            CrateSettings.GATEWAY_RECOVER_AFTER_TIME.defaultValue(),
            CrateSettings.GATEWAY_RECOVERY_AFTER_NODES.defaultValue(),
            CrateSettings.GATEWAY_EXPECTED_NODES.defaultValue()
        ), is(true));
    }

    @Test
    public void testRecoveryAfterTimeCheckWithWrongSetting() {
        RecoveryAfterTimeSysCheck recoveryAfterNodesCheck =
            new RecoveryAfterTimeSysCheck(clusterService, Settings.EMPTY);

        assertThat(recoveryAfterNodesCheck.id(), is(3));
        assertThat(recoveryAfterNodesCheck.severity(), is(SysCheck.Severity.MEDIUM));
        assertThat(recoveryAfterNodesCheck.validate(TimeValue.timeValueMinutes(4), 3, 3), is(false));
    }

    @Test
    public void testValidationDiskWatermarkCheckInBytes() {
        DiskWatermarkNodesSysCheck highDiskWatermarkNodesSysCheck = new HighDiskWatermarkNodesSysCheck(
            clusterService,
            Settings.EMPTY,
            mock(MonitorService.class)
            );

        assertThat(highDiskWatermarkNodesSysCheck.id(), is(5));
        assertThat(highDiskWatermarkNodesSysCheck.severity(), is(SysCheck.Severity.HIGH));

        // default threshold is: 90% used
        assertThat(highDiskWatermarkNodesSysCheck.validate(10, 100), is(true));
        assertThat(highDiskWatermarkNodesSysCheck.validate(9, 100), is(false));
    }
}
