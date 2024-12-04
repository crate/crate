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

package io.crate.expression.reference.sys.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.cluster.gracefulstop.DecommissioningService;
import io.crate.execution.engine.collect.stats.JobsLogService;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class ClusterSettingsExpressionTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testSettingsAreAppliedImmediately() throws Exception {
        Settings settings = Settings.builder().put("bulk.request_timeout", "20s").build();
        clusterService.getClusterSettings().applySettings(settings);
        var sysCluster = SysClusterTableInfo.of(clusterService);

        var expressionFactory = sysCluster.expressions().get(ColumnIdent.of("settings", List.of("bulk", "request_timeout")));
        var expression = expressionFactory.create();
        expression.setNextRow(null);
        assertThat(expression.value()).isEqualTo("20s");
    }

    @Test
    public void testSettingsAreUpdated() throws Exception {
        var sysCluster = SysClusterTableInfo.of(clusterService);

        Settings settings = Settings.builder()
            .put(JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getKey(), 1)
            .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), false)
            .put(DecommissioningService.GRACEFUL_STOP_MIN_AVAILABILITY_SETTING.getKey(), "full")
            .build();

        clusterService.getClusterSettings().applySettings(settings);

        var jobsLogSize = sysCluster.expressions()
            .get(ColumnIdent.of("settings", List.of("stats", "jobs_log_size")))
            .create();
        assertThat(jobsLogSize.value()).isEqualTo(1);

        var statsEnabled = sysCluster.expressions()
            .get(ColumnIdent.of("settings", List.of("stats", "enabled")))
            .create();
        assertThat(statsEnabled.value()).isEqualTo(false);

        var minAvailability = sysCluster.expressions()
            .get(ColumnIdent.of("settings", List.of("cluster", "graceful_stop", "min_availability")))
            .create();
        assertThat(minAvailability.value()).isEqualTo("FULL");
    }
}
