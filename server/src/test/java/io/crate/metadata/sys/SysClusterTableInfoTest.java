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

package io.crate.metadata.sys;


import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.reference.StaticTableReferenceResolver;
import io.crate.metadata.ColumnIdent;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class SysClusterTableInfoTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_routing_allocation_awareness_settings() {
        var clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(
                Settings.builder()
                    .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), List.of("zone"))
                    .put("cluster.routing.allocation.awareness", "force", new String[] {"zone"}, new String[] {"a,b,c"})
                    .build(),
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            THREAD_POOL);
        var clusterTable = SysClusterTableInfo.of(clusterService);

        StaticTableReferenceResolver<Void> refResolver = new StaticTableReferenceResolver<>(clusterTable.expressions());
        NestableCollectExpression<Void, ?> awareness = refResolver.getImplementation(clusterTable.getReference(ColumnIdent.of(
            "settings",
                List.of("cluster", "routing", "allocation", "awareness"))));
        awareness.setNextRow(null);
        assertThat(awareness.value().toString())
            .satisfiesAnyOf(
                s -> assertThat(s).isEqualTo("{attributes=[zone], force={={zone=a,b,c}}}"),
                s -> assertThat(s).isEqualTo("{force={={zone=a,b,c}}, attributes=[zone]}")
            );

        awareness = refResolver.getImplementation(clusterTable.getReference(ColumnIdent.of(
            "settings",
            List.of("cluster", "routing", "allocation", "awareness", "force", ""))));
        awareness.setNextRow(null);
        assertThat(awareness.value().toString()).isEqualTo("{zone=a,b,c}");

        awareness = refResolver.getImplementation(clusterTable.getReference(ColumnIdent.of(
            "settings",
            List.of("cluster", "routing", "allocation", "awareness", "force", "", "zone"))));
        awareness.setNextRow(null);
        assertThat(awareness.value().toString()).isEqualTo("a,b,c");

    }

    @Test
    public void test_license_data_can_be_selected() {
        var clusterTable = SysClusterTableInfo.of(clusterService);

        StaticTableReferenceResolver<Void> refResolver = new StaticTableReferenceResolver<>(clusterTable.expressions());
        NestableCollectExpression<Void, ?> expiryDate = refResolver.getImplementation(clusterTable.getReference(ColumnIdent.of(
            "license",
            "expiry_date")));
        expiryDate.setNextRow(null);
        assertThat(expiryDate.value()).isNull();

        NestableCollectExpression<Void, ?> issuedTo = refResolver.getImplementation(clusterTable.getReference(ColumnIdent.of(
            "license",
            "issued_to")));
        issuedTo.setNextRow(null);
        assertThat(issuedTo.value()).isNull();

        NestableCollectExpression<Void, ?> maxNodes = refResolver.getImplementation(clusterTable.getReference(ColumnIdent.of(
            "license",
            "max_nodes")));
        maxNodes.setNextRow(null);
        assertThat(maxNodes.value()).isNull();
    }

}
