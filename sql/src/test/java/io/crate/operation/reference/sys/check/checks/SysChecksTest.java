/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.operation.reference.sys.check.checks;

import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.reference.sys.check.checks.SysCheck.Severity;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class SysChecksTest extends CrateUnitTest {

    private static ClusterService clusterService = mock(ClusterService.class);

    @Test
    public void testMaxMasterNodesCheckWithEmptySetting() {
        MinMasterNodesSysCheck minMasterNodesCheck = new MinMasterNodesSysCheck(clusterService);
        minMasterNodesCheck.validate(2,
                CrateSettings.GATEWAY_RECOVERY_AFTER_NODES.defaultValue());

        assertThat(minMasterNodesCheck.id(), is(1));
        assertThat(minMasterNodesCheck.severity(), is(Severity.ERROR));
        assertThat(minMasterNodesCheck.passed(), is(false));
    }

    @Test
    public void testMaxMasterNodesCheckWithCorrectSetting() {
        MinMasterNodesSysCheck minMasterNodesCheck = new MinMasterNodesSysCheck(clusterService);
        minMasterNodesCheck.validate(6, 4);

        assertThat(minMasterNodesCheck.id(), is(1));
        assertThat(minMasterNodesCheck.severity(), is(Severity.INFO));
        assertThat(minMasterNodesCheck.passed(), is(true));
    }

    @Test
    public void testMaxMasterNodesCheckWithLessThanQuorum() {
        MinMasterNodesSysCheck minMasterNodesCheck = new MinMasterNodesSysCheck(clusterService);
        minMasterNodesCheck.validate(6, 3);

        assertThat(minMasterNodesCheck.id(), is(1));
        assertThat(minMasterNodesCheck.severity(), is(Severity.ERROR));
        assertThat(minMasterNodesCheck.passed(), is(false));
    }

    @Test
    public void testMaxMasterNodesCheckWithGreaterThanNodes() {
        MinMasterNodesSysCheck minMasterNodesCheck = new MinMasterNodesSysCheck(clusterService);
        minMasterNodesCheck.validate(6, 7);

        assertThat(minMasterNodesCheck.id(), is(1));
        assertThat(minMasterNodesCheck.severity(), is(Severity.ERROR));
        assertThat(minMasterNodesCheck.passed(), is(false));
    }

    @Test
    public void testRecoveryExpectedNodesCheckWithDefaultSetting() {
        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck =
                new RecoveryExpectedNodesSysCheck(clusterService, ImmutableSettings.EMPTY);
        recoveryExpectedNodesCheck.validate(1,
                CrateSettings.GATEWAY_EXPECTED_NODES.defaultValue());

        assertThat(recoveryExpectedNodesCheck.id(), is(2));
        assertThat(recoveryExpectedNodesCheck.severity(), is(Severity.ERROR));
        assertThat(recoveryExpectedNodesCheck.passed(), is(false));
    }

    @Test
    public void testRecoveryExpectedNodesCheckWithLessThanQuorum() {
        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck =
                new RecoveryExpectedNodesSysCheck(clusterService, ImmutableSettings.EMPTY);
        recoveryExpectedNodesCheck.validate(2, 1);

        assertThat(recoveryExpectedNodesCheck.id(), is(2));
        assertThat(recoveryExpectedNodesCheck.severity(), is(Severity.ERROR));
        assertThat(recoveryExpectedNodesCheck.passed(), is(false));
    }

    @Test
    public void testRecoveryExpectedNodesCheckWithCorrectSetting() {
        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck =
                new RecoveryExpectedNodesSysCheck(clusterService, ImmutableSettings.EMPTY);
        recoveryExpectedNodesCheck.validate(3, 3);

        assertThat(recoveryExpectedNodesCheck.id(), is(2));
        assertThat(recoveryExpectedNodesCheck.severity(), is(Severity.INFO));
        assertThat(recoveryExpectedNodesCheck.passed(), is(true));
    }

    @Test
    public void testRecoveryExpectedNodesCheckWithBiggerThanNumberOfNodes() {
        RecoveryExpectedNodesSysCheck recoveryExpectedNodesCheck =
                new RecoveryExpectedNodesSysCheck(clusterService, ImmutableSettings.EMPTY);
        recoveryExpectedNodesCheck.validate(3, 4);

        assertThat(recoveryExpectedNodesCheck.id(), is(2));
        assertThat(recoveryExpectedNodesCheck.severity(), is(Severity.ERROR));
        assertThat(recoveryExpectedNodesCheck.passed(), is(false));
    }

    @Test
    public void testRecoveryAfterNodesCheckWithDefaultSetting() {
        RecoveryAfterNodesSysCheck recoveryAfterNodesCheck =
                new RecoveryAfterNodesSysCheck(clusterService, ImmutableSettings.EMPTY);
        recoveryAfterNodesCheck.validate(1,
                CrateSettings.GATEWAY_RECOVERY_AFTER_NODES.defaultValue(),
                CrateSettings.GATEWAY_EXPECTED_NODES.defaultValue()
        );

        assertThat(recoveryAfterNodesCheck.id(), is(3));
        assertThat(recoveryAfterNodesCheck.severity(), is(Severity.ERROR));
        assertThat(recoveryAfterNodesCheck.passed(), is(false));
    }

    @Test
    public void testRecoveryAfterNodesCheckWithLessThanQuorum() {
        RecoveryAfterNodesSysCheck recoveryAfterNodesCheck =
                new RecoveryAfterNodesSysCheck(clusterService, ImmutableSettings.EMPTY);
        recoveryAfterNodesCheck.validate(8, 4, 8);

        assertThat(recoveryAfterNodesCheck.id(), is(3));
        assertThat(recoveryAfterNodesCheck.severity(), is(Severity.ERROR));
        assertThat(recoveryAfterNodesCheck.passed(), is(false));
    }

    @Test
    public void testRecoveryAfterNodesCheckWithGreaterThanNodes() {
        RecoveryAfterNodesSysCheck recoveryAfterNodesCheck =
                new RecoveryAfterNodesSysCheck(clusterService, ImmutableSettings.EMPTY);
        recoveryAfterNodesCheck.validate(8, 9, 8);

        assertThat(recoveryAfterNodesCheck.id(), is(3));
        assertThat(recoveryAfterNodesCheck.severity(), is(Severity.ERROR));
        assertThat(recoveryAfterNodesCheck.passed(), is(false));
    }

    @Test
    public void testRecoveryAfterNodesCheckWithCorrectSetting() {
        RecoveryAfterNodesSysCheck recoveryAfterNodesCheck =
                new RecoveryAfterNodesSysCheck(clusterService, ImmutableSettings.EMPTY);
        recoveryAfterNodesCheck.validate(8, 5, 8);

        assertThat(recoveryAfterNodesCheck.id(), is(3));
        assertThat(recoveryAfterNodesCheck.severity(), is(Severity.INFO));
        assertThat(recoveryAfterNodesCheck.passed(), is(true));
    }

    @Test
    public void testRecoveryAfterTimeCheckWithCorrectSetting() {
        RecoveryAfterTimeSysCheck recoveryAfterNodesCheck =
                new RecoveryAfterTimeSysCheck(ImmutableSettings.EMPTY);
        recoveryAfterNodesCheck.validate(TimeValue.timeValueMinutes(6), 1, 4);

        assertThat(recoveryAfterNodesCheck.id(), is(4));
        assertThat(recoveryAfterNodesCheck.severity(), is(Severity.INFO));
        assertThat(recoveryAfterNodesCheck.passed(), is(true));
    }

    @Test
    public void testRecoveryAfterTimeCheckWithDefaultSetting() {
        RecoveryAfterTimeSysCheck recoveryAfterNodesCheck =
                new RecoveryAfterTimeSysCheck(ImmutableSettings.EMPTY);
        recoveryAfterNodesCheck.validate(
                CrateSettings.GATEWAY_RECOVER_AFTER_TIME.defaultValue(),
                CrateSettings.GATEWAY_RECOVERY_AFTER_NODES.defaultValue(),
                CrateSettings.GATEWAY_EXPECTED_NODES.defaultValue()
        );

        assertThat(recoveryAfterNodesCheck.id(), is(4));
        assertThat(recoveryAfterNodesCheck.severity(), is(Severity.ERROR));
        assertThat(recoveryAfterNodesCheck.passed(), is(false));
    }

    @Test
    public void testRecoveryAfterTimeCheckWithWrongSetting() {
        RecoveryAfterTimeSysCheck recoveryAfterNodesCheck =
                new RecoveryAfterTimeSysCheck(ImmutableSettings.EMPTY);
        recoveryAfterNodesCheck.validate(TimeValue.timeValueMinutes(4), 3, 3);

        assertThat(recoveryAfterNodesCheck.id(), is(4));
        assertThat(recoveryAfterNodesCheck.severity(), is(Severity.ERROR));
        assertThat(recoveryAfterNodesCheck.passed(), is(false));
    }

}
