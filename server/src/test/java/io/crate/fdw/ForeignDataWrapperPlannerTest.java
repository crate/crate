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

package io.crate.fdw;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.Mockito;

import io.crate.data.Row;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.planner.CreateServerPlan;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class ForeignDataWrapperPlannerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_creating_foreign_table_with_invalid_name_fails() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .build();

        assertThatThrownBy(() -> e.plan("create foreign table sys.nope (x int) server pg"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot create relation in read-only schema: sys");
    }

    @Test
    public void test_create_server_fails_if_mandatory_options_are_missing() throws Exception {
        var e = SQLExecutor.builder(clusterService).build();
        CreateServerPlan plan = e.plan("create server pg foreign data wrapper jdbc");
        var testingRowConsumer = new TestingRowConsumer();
        plan.execute(
            Mockito.mock(DependencyCarrier.class),
            e.getPlannerContext(clusterService.state()),
            testingRowConsumer,
            Row.EMPTY,
            SubQueryResults.EMPTY
        );
        assertThat(testingRowConsumer.completionFuture())
            .failsWithin(0, TimeUnit.SECONDS)
            .withThrowableThat()
            .havingRootCause()
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .withMessage("Mandatory server option `url` for foreign data wrapper `jdbc` is missing");

    }

    @Test
    public void test_cannot_use_unsupported_options_in_create_server() throws Exception {
        var e = SQLExecutor.builder(clusterService).build();
        CreateServerPlan plan = e.plan("create server pg foreign data wrapper jdbc options (url '', wrong_option 10)");
        var testingRowConsumer = new TestingRowConsumer();
        plan.execute(
            Mockito.mock(DependencyCarrier.class),
            e.getPlannerContext(clusterService.state()),
            testingRowConsumer,
            Row.EMPTY,
            SubQueryResults.EMPTY
        );
        assertThat(testingRowConsumer.completionFuture())
            .failsWithin(0, TimeUnit.SECONDS)
            .withThrowableThat()
            .havingRootCause()
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .withMessage("Unsupported server options for foreign data wrapper `jdbc`: wrong_option. Valid options are: url");
    }

    @Test
    public void test_cannot_create_server_if_fdw_is_missing() throws Exception {
        var e = SQLExecutor.builder(clusterService).build();
        String stmt = "create server pg foreign data wrapper dummy options (host 'localhost', dbname 'doc', port '5432')";
        CreateServerPlan plan = e.plan(stmt);

        var testingRowConsumer = new TestingRowConsumer();
        plan.execute(
            Mockito.mock(DependencyCarrier.class),
            e.getPlannerContext(clusterService.state()),
            testingRowConsumer,
            Row.EMPTY,
            SubQueryResults.EMPTY
        );
        assertThat(testingRowConsumer.completionFuture())
            .failsWithin(0, TimeUnit.SECONDS)
            .withThrowableThat()
            .havingRootCause()
            .withMessageContaining("foreign-data wrapper dummy does not exist");
    }
}
