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

import com.google.common.collect.ImmutableList;
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.reference.sys.check.SysCheck;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.fs.FsProbe;
import org.junit.Test;
import org.mockito.Answers;

import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SysNodeChecksTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testRecoveryExpectedNodesCheckWithDefaultSetting() {
        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck =
            new RecoveryExpectedNodesSysCheck(dummyClusterService, Settings.EMPTY);

        assertThat(recoveryExpectedNodesCheck.id(), is(1));
        assertThat(recoveryExpectedNodesCheck.nodeId().utf8ToString(), is("noop_id"));
        assertThat(recoveryExpectedNodesCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(recoveryExpectedNodesCheck.validate(1,
            CrateSettings.GATEWAY_EXPECTED_NODES.defaultValue()), is(true));
    }

    @Test
    public void testRecoveryExpectedNodesCheckWithLessThanQuorum() {
        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck =
            new RecoveryExpectedNodesSysCheck(dummyClusterService, Settings.EMPTY);

        assertThat(recoveryExpectedNodesCheck.id(), is(1));
        assertThat(recoveryExpectedNodesCheck.nodeId().utf8ToString(), is("noop_id"));
        assertThat(recoveryExpectedNodesCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(recoveryExpectedNodesCheck.validate(2, 1), is(false));
    }

    @Test
    public void testRecoveryExpectedNodesCheckWithCorrectSetting() {
        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck =
            new RecoveryExpectedNodesSysCheck(dummyClusterService, Settings.EMPTY);

        assertThat(recoveryExpectedNodesCheck.id(), is(1));
        assertThat(recoveryExpectedNodesCheck.nodeId().utf8ToString(), is("noop_id"));
        assertThat(recoveryExpectedNodesCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(recoveryExpectedNodesCheck.validate(3, 3), is(true));
    }

    @Test
    public void testRecoveryExpectedNodesCheckWithBiggerThanNumberOfNodes() {
        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck =
            new RecoveryExpectedNodesSysCheck(dummyClusterService, Settings.EMPTY);

        assertThat(recoveryExpectedNodesCheck.id(), is(1));
        assertThat(recoveryExpectedNodesCheck.nodeId().utf8ToString(), is("noop_id"));
        assertThat(recoveryExpectedNodesCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(recoveryExpectedNodesCheck.validate(3, 4), is(false));
    }

    @Test
    public void testRecoveryAfterNodesCheckWithDefaultSetting() {
        RecoveryAfterNodesSysCheck recoveryAfterNodesCheck =
            new RecoveryAfterNodesSysCheck(dummyClusterService, Settings.EMPTY);

        assertThat(recoveryAfterNodesCheck.id(), is(2));
        assertThat(recoveryAfterNodesCheck.nodeId().utf8ToString(), is("noop_id"));
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

        when(clusterService.localNode().getId()).thenReturn("noop_id");
        when(clusterService.state().nodes().getSize()).thenReturn(8);

        assertThat(recoveryAfterNodesCheck.id(), is(2));
        assertThat(recoveryAfterNodesCheck.nodeId().utf8ToString(), is("noop_id"));
        assertThat(recoveryAfterNodesCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(recoveryAfterNodesCheck.validate(4, 8), is(false));
    }

    @Test
    public void testRecoveryAfterNodesCheckWithCorrectSetting() {
        ClusterService clusterService = mock(ClusterService.class, Answers.RETURNS_DEEP_STUBS.get());

        RecoveryAfterNodesSysCheck recoveryAfterNodesCheck =
            new RecoveryAfterNodesSysCheck(clusterService, Settings.EMPTY);

        when(clusterService.localNode().getId()).thenReturn("noop_id");
        when(clusterService.state().nodes().getSize()).thenReturn(8);

        assertThat(recoveryAfterNodesCheck.id(), is(2));
        assertThat(recoveryAfterNodesCheck.nodeId().utf8ToString(), is("noop_id"));
        assertThat(recoveryAfterNodesCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(recoveryAfterNodesCheck.validate(8, 8), is(true));
    }

    @Test
    public void testRecoveryAfterTimeCheckWithCorrectSetting() {
        RecoveryAfterTimeSysCheck recoveryAfterNodesCheck =
            new RecoveryAfterTimeSysCheck(dummyClusterService, Settings.EMPTY);

        assertThat(recoveryAfterNodesCheck.id(), is(3));
        assertThat(recoveryAfterNodesCheck.nodeId().utf8ToString(), is("noop_id"));
        assertThat(recoveryAfterNodesCheck.severity(), is(SysCheck.Severity.MEDIUM));
        assertThat(recoveryAfterNodesCheck.validate(TimeValue.timeValueMinutes(6), 1, 4), is(true));
    }

    @Test
    public void testRecoveryAfterTimeCheckWithDefaultSetting() {
        RecoveryAfterTimeSysCheck recoveryAfterNodesCheck =
            new RecoveryAfterTimeSysCheck(dummyClusterService, Settings.EMPTY);

        assertThat(recoveryAfterNodesCheck.id(), is(3));
        assertThat(recoveryAfterNodesCheck.nodeId().utf8ToString(), is("noop_id"));
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
            new RecoveryAfterTimeSysCheck(dummyClusterService, Settings.EMPTY);

        assertThat(recoveryAfterNodesCheck.id(), is(3));
        assertThat(recoveryAfterNodesCheck.nodeId().utf8ToString(), is("noop_id"));
        assertThat(recoveryAfterNodesCheck.severity(), is(SysCheck.Severity.MEDIUM));
        assertThat(recoveryAfterNodesCheck.validate(TimeValue.timeValueMinutes(4), 3, 3), is(false));
    }

    private final FsInfo fsInfo = mock(FsInfo.class);

    @Test
    public void testValidationDiskWatermarkCheckInBytes() {
        DiskWatermarkNodesSysCheck highDiskWatermarkNodesSysCheck = new HighDiskWatermarkNodesSysCheck(
            dummyClusterService,
            Settings.EMPTY,
            mock(FsProbe.class),
            mock(FsInfo.class));

        assertThat(highDiskWatermarkNodesSysCheck.id(), is(5));
        assertThat(highDiskWatermarkNodesSysCheck.nodeId().utf8ToString(), is("noop_id"));
        assertThat(highDiskWatermarkNodesSysCheck.severity(), is(SysCheck.Severity.HIGH));

        // Percentage values refer to used disk space
        // sda: 160b is available
        // sdc: 140b is available
        List<FsInfo.Path> sameSizeDisksGroup = ImmutableList.of(
            new FsInfo.Path("/middle", "/dev/sda", 300, 170, 160),
            new FsInfo.Path("/most", "/dev/sdc", 300, 150, 140)
        );

        // disk.watermark.high: 139b
        when(fsInfo.iterator()).thenReturn(sameSizeDisksGroup.iterator());
        assertThat(highDiskWatermarkNodesSysCheck.validate(fsInfo, 100.0, 139), is(true));

        // disk.watermark.high: 140b
        when(fsInfo.iterator()).thenReturn(sameSizeDisksGroup.iterator());
        assertThat(highDiskWatermarkNodesSysCheck.validate(fsInfo, 100.0, 150), is(false));
    }

    @Test
    public void testValidationDiskWatermarkCheckInPercents() {
        DiskWatermarkNodesSysCheck lowDiskWatermarkNodesSysCheck = new LowDiskWatermarkNodesSysCheck(
            dummyClusterService,
            Settings.EMPTY,
            mock(FsProbe.class),
            mock(FsInfo.class));

        assertThat(lowDiskWatermarkNodesSysCheck.id(), is(6));
        assertThat(lowDiskWatermarkNodesSysCheck.nodeId().utf8ToString(), is("noop_id"));
        assertThat(lowDiskWatermarkNodesSysCheck.severity(), is(SysCheck.Severity.HIGH));

        // Percentage values refer to used disk space
        // sda: 70% is used
        // sdc: 50% is used
        List<FsInfo.Path> differentSizeDisksGroup = ImmutableList.of(
            new FsInfo.Path("/middle", "/dev/sda", 100, 40, 30),
            new FsInfo.Path("/most", "/dev/sdc", 300, 130, 150)
        );

        // disk.watermark.high: 75%
        when(fsInfo.iterator()).thenReturn(differentSizeDisksGroup.iterator());
        assertThat(lowDiskWatermarkNodesSysCheck.validate(fsInfo, 75.0, 0), is(true));

        // disk.watermark.high: 55%
        when(fsInfo.iterator()).thenReturn(differentSizeDisksGroup.iterator());
        assertThat(lowDiskWatermarkNodesSysCheck.validate(fsInfo, 55.0, 0), is(false));
    }
}
