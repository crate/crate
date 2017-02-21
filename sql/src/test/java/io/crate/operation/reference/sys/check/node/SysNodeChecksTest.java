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
import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.fs.FsProbe;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.junit.Test;
import org.mockito.Answers;

import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SysNodeChecksTest extends CrateUnitTest {

    private final ClusterService clusterService = new NoopClusterService();

    private RecoveryExpectedNodesSysCheck recoveryExpectedNodesSysCheck() {
        return applyAndAssertNodeId(new RecoveryExpectedNodesSysCheck(clusterService, Settings.EMPTY));
    }

    private <T extends SysNodeCheck> T applyAndAssertNodeId(T check){
        check.setNodeId(new BytesRef(clusterService.localNode().getId()));
        assertThat(check.nodeId().utf8ToString(), is("noop_id"));
        return check;
    }

    @Test
    public void testRecoveryExpectedNodesCheckWithDefaultSetting() {
        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck = recoveryExpectedNodesSysCheck();

        assertThat(recoveryExpectedNodesCheck.id(), is(1));
        assertThat(recoveryExpectedNodesCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(recoveryExpectedNodesCheck.validate(1,
            CrateSettings.GATEWAY_EXPECTED_NODES.defaultValue()), is(true));
    }


    @Test
    public void testRecoveryExpectedNodesCheckWithLessThanQuorum() {
        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck = recoveryExpectedNodesSysCheck();

        assertThat(recoveryExpectedNodesCheck.id(), is(1));
        assertThat(recoveryExpectedNodesCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(recoveryExpectedNodesCheck.validate(2, 1), is(false));
    }

    @Test
    public void testRecoveryExpectedNodesCheckWithCorrectSetting() {
        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck = recoveryExpectedNodesSysCheck();

        assertThat(recoveryExpectedNodesCheck.id(), is(1));
        assertThat(recoveryExpectedNodesCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(recoveryExpectedNodesCheck.validate(3, 3), is(true));
    }

    @Test
    public void testRecoveryExpectedNodesCheckWithBiggerThanNumberOfNodes() {
        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck = recoveryExpectedNodesSysCheck();

        assertThat(recoveryExpectedNodesCheck.id(), is(1));
        assertThat(recoveryExpectedNodesCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(recoveryExpectedNodesCheck.validate(3, 4), is(false));
    }

    @Test
    public void testRecoveryAfterNodesCheckWithDefaultSetting() {
        RecoveryAfterNodesSysCheck recoveryAfterNodesCheck = applyAndAssertNodeId(
            new RecoveryAfterNodesSysCheck(clusterService, Settings.EMPTY));

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

        RecoveryAfterNodesSysCheck recoveryAfterNodesCheck = new RecoveryAfterNodesSysCheck(clusterService, Settings.EMPTY);

        when(clusterService.state().nodes().getSize()).thenReturn(8);

        assertThat(recoveryAfterNodesCheck.id(), is(2));
        assertThat(recoveryAfterNodesCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(recoveryAfterNodesCheck.validate(4, 8), is(false));
    }

    @Test
    public void testRecoveryAfterNodesCheckWithCorrectSetting() {
        ClusterService clusterService = mock(ClusterService.class, Answers.RETURNS_DEEP_STUBS.get());

        RecoveryAfterNodesSysCheck recoveryAfterNodesCheck = applyAndAssertNodeId(
            new RecoveryAfterNodesSysCheck(clusterService, Settings.EMPTY));

        when(clusterService.state().nodes().getSize()).thenReturn(8);

        assertThat(recoveryAfterNodesCheck.id(), is(2));
        assertThat(recoveryAfterNodesCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(recoveryAfterNodesCheck.validate(8, 8), is(true));
    }

    @Test
    public void testRecoveryAfterTimeCheckWithCorrectSetting() {
        RecoveryAfterTimeSysCheck recoveryAfterNodesCheck =
            applyAndAssertNodeId(new RecoveryAfterTimeSysCheck(clusterService, Settings.EMPTY));

        assertThat(recoveryAfterNodesCheck.id(), is(3));
        assertThat(recoveryAfterNodesCheck.severity(), is(SysCheck.Severity.MEDIUM));
        assertThat(recoveryAfterNodesCheck.validate(TimeValue.timeValueMinutes(6), 1, 4), is(true));
    }

    @Test
    public void testRecoveryAfterTimeCheckWithDefaultSetting() {
        RecoveryAfterTimeSysCheck recoveryAfterNodesCheck =
            applyAndAssertNodeId(new RecoveryAfterTimeSysCheck(clusterService, Settings.EMPTY));

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
            applyAndAssertNodeId(new RecoveryAfterTimeSysCheck(clusterService, Settings.EMPTY));

        assertThat(recoveryAfterNodesCheck.id(), is(3));
        assertThat(recoveryAfterNodesCheck.severity(), is(SysCheck.Severity.MEDIUM));
        assertThat(recoveryAfterNodesCheck.validate(TimeValue.timeValueMinutes(4), 3, 3), is(false));
    }

    @Test
    public void testValidationDiskWatermarkCheckInBytes() {
        DiskWatermarkNodesSysCheck highDiskWatermark
            = new HighDiskWatermarkNodesSysCheck(clusterService, mock(Provider.class), mock(FsProbe.class));

        assertThat(highDiskWatermark.id(), is(5));
        assertThat(highDiskWatermark.severity(), is(SysCheck.Severity.HIGH));

        DiskThresholdDecider decider = mock(DiskThresholdDecider.class);
        // disk.watermark.high: 170b
        // A path must have at least 170 bytes to pass the check, only 160 bytes are available.
        when(decider.getFreeDiskThresholdHigh()).thenReturn(.0);
        when(decider.getFreeBytesThresholdHigh()).thenReturn(new ByteSizeValue(170));
        assertThat(highDiskWatermark.validate(decider, 160, 300), is(false));

        // disk.watermark.high: 130b
        // A path must have at least 130 bytes to pass the check, 140 bytes available.
        when(decider.getFreeDiskThresholdHigh()).thenReturn(.0);
        when(decider.getFreeBytesThresholdHigh()).thenReturn(new ByteSizeValue(130));
        assertThat(highDiskWatermark.validate(decider, 140, 300), is(true));
    }

    @Test
    public void testValidationDiskWatermarkCheckInPercents() {
        DiskWatermarkNodesSysCheck lowDiskWatermark
            = new LowDiskWatermarkNodesSysCheck(clusterService, mock(Provider.class), mock(FsProbe.class));
        assertThat(lowDiskWatermark.id(), is(6));
        assertThat(lowDiskWatermark.severity(), is(SysCheck.Severity.HIGH));

        DiskThresholdDecider decider = mock(DiskThresholdDecider.class);
        // disk.watermark.low: 75%. It must fail when at least 75% of disk is used.
        // Free - 150 bytes, total - 300 bytes. 50% of disk is used.
        // freeDiskThresholdLow = 100.0 - 75.0
        when(decider.getFreeDiskThresholdLow()).thenReturn(25.);
        when(decider.getFreeBytesThresholdLow()).thenReturn(new ByteSizeValue(0));

        assertThat(lowDiskWatermark.validate(decider, 150, 300), is(true));

        // disk.watermark.low: 45%. The check must fail when at least 45% of disk is used.
        // Free - 30 bytes, Total - 100 bytes. 70% of disk is used.
        // freeDiskThresholdLow = 100.0 - 45.0
        when(decider.getFreeDiskThresholdLow()).thenReturn(55.);
        when(decider.getFreeBytesThresholdLow()).thenReturn(new ByteSizeValue(0));
        assertThat(lowDiskWatermark.validate(decider, 30, 100), is(false));
    }

    @Test
    public void testGetLeastAvailablePathForDiskWatermarkChecks() throws IOException {
        FsProbe fsProbe = mock(FsProbe.class);
        FsInfo fsInfo = mock(FsInfo.class);
        when(fsProbe.stats()).thenReturn(fsInfo);
        when(fsInfo.iterator()).thenReturn(ImmutableList.of(
            new FsInfo.Path("/middle", "/dev/sda", 300, 170, 160),
            new FsInfo.Path("/most", "/dev/sdc", 300, 150, 140)
        ).iterator());

        DiskWatermarkNodesSysCheck diskWatermark
            = new LowDiskWatermarkNodesSysCheck(clusterService, mock(Provider.class), fsProbe);
        assertThat(diskWatermark.getLeastAvailablePath().getAvailable().getBytes(), is(140L));
    }
}
