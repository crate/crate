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

package io.crate.planner.consumer;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import io.crate.analyze.TableDefinitions;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.service.ClusterService;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class InsertFromSubQueryPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = buildExecutor(clusterService);
    }

    private static SQLExecutor buildExecutor(ClusterService clusterService) throws IOException {
        return SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
    }

    @Test
    public void test_returning_for_insert_from_values_throw_error_with_4_1_nodes() throws Exception {
        // Make sure the former initialized cluster service is shutdown
        cleanup();
        this.clusterService = createClusterService(additionalClusterSettings(), Version.V_4_1_0);
        e = buildExecutor(clusterService);
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage(InsertFromSubQueryPlanner.RETURNING_VERSION_ERROR_MSG);
        e.plan("insert into users (id, name) values (1, 'bob') returning id");

    }

    @Test
    public void test_returning_for_insert_from_subquery_throw_error_with_4_1_nodes() throws Exception {
        // Make sure the former initialized cluster service is shutdown
        cleanup();
        this.clusterService = createClusterService(additionalClusterSettings(), Version.V_4_1_0);
        e = buildExecutor(clusterService);
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage(InsertFromSubQueryPlanner.RETURNING_VERSION_ERROR_MSG);
        e.plan("insert into users (id, name) select '1' as id, 'b' as name returning id");
    }

}
