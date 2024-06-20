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

package io.crate.planner.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.TableDefinitions;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.planner.Merge;
import io.crate.planner.node.dql.Collect;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class InsertFromSubQueryPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = buildExecutor(clusterService);
    }

    private static SQLExecutor buildExecutor(ClusterService clusterService) throws IOException {
        return SQLExecutor.builder(clusterService)
            .setNumNodes(2)
            .build()
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addTable("create table target (id int, name varchar)");
    }

    @Test
    public void test_returning_for_insert_from_values_throw_error_with_4_1_nodes() throws Exception {
        // Make sure the former initialized cluster service is shutdown
        cleanup();
        this.clusterService = createClusterService(
            additionalClusterSettings(), Metadata.EMPTY_METADATA, Version.V_4_1_0);
        e = buildExecutor(clusterService);
        assertThatThrownBy(() -> e.plan("insert into users (id, name) values (1, 'bob') returning id"))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage(InsertFromSubQueryPlanner.RETURNING_VERSION_ERROR_MSG);
    }

    @Test
    public void test_returning_for_insert_from_subquery_throw_error_with_4_1_nodes() throws Exception {
        // Make sure the former initialized cluster service is shutdown
        cleanup();
        this.clusterService = createClusterService(additionalClusterSettings(), Metadata.EMPTY_METADATA,
                Version.V_4_1_0);
        e = buildExecutor(clusterService);
        assertThatThrownBy(() ->
                e.plan("insert into users (id, name) select '1' as id, 'b' as name returning id"))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage(InsertFromSubQueryPlanner.RETURNING_VERSION_ERROR_MSG);
    }

    @Test
    public void test_insert_from_subquery_with_order_by_symbols_match_collect_symbols() {
        // Ensures that order by symbols may also be rewritten to source lookup refs if collect symbols are rewritten
        Merge localMerge = e.plan("insert into target (id, name) " +
                                  "select id, name from users order by id, name");
        Collect collect = (Collect) localMerge.subPlan();
        RoutedCollectPhase collectPhase = (RoutedCollectPhase) collect.collectPhase();
        assertThat(collectPhase.orderBy().orderBySymbols()).isEqualTo(collectPhase.toCollect());
    }
}
