/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.reference.sys;


import io.crate.metadata.ClusterReferenceResolver;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.reference.NestedObjectExpression;
import io.crate.operation.reference.sys.cluster.SysClusterExpressionModule;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static io.crate.testing.TestingHelpers.refInfo;
import static org.hamcrest.Matchers.is;

public class TestGlobalSysExpressions extends CrateDummyClusterServiceUnitTest {

    private ClusterReferenceResolver resolver;

    @Before
    public void prepare() throws Exception {
        Injector injector = new ModulesBuilder()
            .add(new SysClusterExpressionModule())
            .add((Module) binder -> {
                binder.bind(ClusterService.class).toInstance(clusterService);
                binder.bind(Settings.class).toInstance(Settings.EMPTY);
                binder.bind(ClusterReferenceResolver.class).asEagerSingleton();
            }).createInjector();
        resolver = injector.getInstance(ClusterReferenceResolver.class);
    }

    @Test
    public void testClusterSettings() throws Exception {
        Reference refInfo = refInfo("sys.cluster.settings", DataTypes.OBJECT, RowGranularity.CLUSTER);
        NestedObjectExpression settingsExpression = (NestedObjectExpression) resolver.getImplementation(refInfo);

        Map settings = settingsExpression.value();

        Map stats = (Map) settings.get(CrateSettings.STATS.name());
        assertThat(stats.get(CrateSettings.STATS_ENABLED.name()),
            is(CrateSettings.STATS_ENABLED.defaultValue()));
        assertThat(stats.get(CrateSettings.STATS_JOBS_LOG_SIZE.name()),
            is(CrateSettings.STATS_JOBS_LOG_SIZE.defaultValue()));
        assertThat(stats.get(CrateSettings.STATS_OPERATIONS_LOG_SIZE.name()),
            is(CrateSettings.STATS_OPERATIONS_LOG_SIZE.defaultValue()));
        Map statsBreakerLog = (Map) ((Map) stats.get(CrateSettings.STATS_BREAKER.name()))
            .get(CrateSettings.STATS_BREAKER_LOG.name());
        Map statsBreakerLogJobs = (Map) statsBreakerLog.get(CrateSettings.STATS_BREAKER_LOG_JOBS.name());
        assertThat(statsBreakerLogJobs.get(CrateSettings.STATS_BREAKER_LOG_JOBS_LIMIT.name()),
            is(CrateSettings.STATS_BREAKER_LOG_JOBS_LIMIT.defaultValue().toString()));

        Map cluster = (Map) settings.get(CrateSettings.CLUSTER.name());
        Map gracefulStop = (Map) cluster.get(CrateSettings.GRACEFUL_STOP.name());
        assertThat(
            gracefulStop.get(CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.name()),
            is(CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.defaultValue()));
        assertThat(
            gracefulStop.get(CrateSettings.GRACEFUL_STOP_REALLOCATE.name()),
            is(CrateSettings.GRACEFUL_STOP_REALLOCATE.defaultValue()));
        assertThat(
            gracefulStop.get(CrateSettings.GRACEFUL_STOP_TIMEOUT.name()),
            is(CrateSettings.GRACEFUL_STOP_TIMEOUT.defaultValue().toString())
        );
        assertThat(
            gracefulStop.get(CrateSettings.GRACEFUL_STOP_FORCE.name()),
            is(CrateSettings.GRACEFUL_STOP_FORCE.defaultValue())
        );
        assertThat(
            gracefulStop.get(CrateSettings.GRACEFUL_STOP_TIMEOUT.name()),
            is(CrateSettings.GRACEFUL_STOP_TIMEOUT.defaultValue().toString())
        );
        Map routing = (Map) cluster.get(CrateSettings.ROUTING.name());
        Map routingAllocation = (Map) routing.get(CrateSettings.ROUTING_ALLOCATION.name());
        assertThat(
            routingAllocation.get(CrateSettings.ROUTING_ALLOCATION_ENABLE.name()),
            is(CrateSettings.ROUTING_ALLOCATION_ENABLE.defaultValue())
        );

        Map gateway = (Map) settings.get(CrateSettings.GATEWAY.name());
        assertThat(gateway.get(CrateSettings.GATEWAY_RECOVER_AFTER_TIME.name()),
            is(CrateSettings.GATEWAY_RECOVER_AFTER_TIME.defaultValue().toString()));
        assertThat(gateway.get(CrateSettings.GATEWAY_RECOVERY_AFTER_NODES.name()),
            is(CrateSettings.GATEWAY_RECOVERY_AFTER_NODES.defaultValue()));
        assertThat(gateway.get(CrateSettings.GATEWAY_EXPECTED_NODES.name()),
            is(CrateSettings.GATEWAY_EXPECTED_NODES.defaultValue()));

        Map licence = (Map) settings.get(CrateSettings.LICENSE.name());
        assertThat(licence.get(CrateSettings.LICENSE_ENTERPRISE.name()),
            is(CrateSettings.LICENSE_ENTERPRISE.defaultValue()));
    }
}
