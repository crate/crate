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

package io.crate.executor.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.ddl.UpdateSettingsPlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.Assignment;

public class DependencyCarrierDDLTest extends IntegTestCase {

    private DependencyCarrier executor;

    @Before
    public void transportSetup() {
        executor = cluster().getInstance(DependencyCarrier.class);
    }

    @After
    public void resetSettings() throws Exception {
        execute("reset global stats.enabled, bulk.request_timeout");
    }


    /**
     * this case should not happen as closed indices aren't listed as TableInfo
     * but if it does maybe because of stale cluster state - validate behaviour here
     * <p>
     * cannot prevent this task from deleting closed indices.
     */
    @Test
    public void testDeletePartitionTaskClosed() throws Exception {
        execute("create table t (id integer primary key, name string) partitioned by (id)");
        ensureYellow();

        execute("insert into t (id, name) values (1, 'Ford')");
        assertThat(response.rowCount()).isEqualTo(1L);
        ensureYellow();

        PlanForNode plan = plan("delete from t where id = ?");

        execute("alter table t partition (id = 1) close");

        Bucket bucket = executePlan(plan.plan, plan.plannerContext, new Row1(1));
        assertThat(bucket).containsExactly(new Row1(-1L));

        execute("select * from information_schema.table_partitions where table_name = 't'");
        assertThat(response.rowCount()).isEqualTo(0L);
    }

    @Test
    public void testClusterUpdateSettingsTask() throws Exception {
        final String persistentSetting = "stats.enabled";
        final String transientSetting = "bulk.request_timeout";

        // Update persistent only
        List<Assignment<Symbol>> persistentSettings = List.of(
            new Assignment<>(Literal.of(persistentSetting), List.of(Literal.of(false))));

        UpdateSettingsPlan node = new UpdateSettingsPlan(persistentSettings, true);
        PlannerContext plannerContext = mock(PlannerContext.class);
        Bucket objects = executePlan(node, plannerContext);

        assertThat(objects).containsExactly(new Row1(1L));
        var stateResponse = client().admin().cluster().state(new ClusterStateRequest()).get();
        assertThat(stateResponse.getState().metadata()
            .persistentSettings().get(persistentSetting)
        ).isEqualTo("false");

        // Update transient only
        List<Assignment<Symbol>> transientSettings = List.of(
            new Assignment<>(Literal.of(transientSetting), List.of(Literal.of("123s"))));

        node = new UpdateSettingsPlan(transientSettings, false);
        objects = executePlan(node, plannerContext);

        assertThat(objects).containsExactly(new Row1(1L));
        stateResponse = client().admin().cluster().state(new ClusterStateRequest()).get();
        assertThat(stateResponse.getState().metadata()
            .transientSettings().get(transientSetting)
        ).isEqualTo("123s");
    }

    private Bucket executePlan(Plan plan, PlannerContext plannerContext, Row params) throws Exception {
        TestingRowConsumer consumer = new TestingRowConsumer();
        plan.execute(
            executor,
            plannerContext,
            consumer,
            params,
            SubQueryResults.EMPTY
        );
        return consumer.getBucket();
    }

    private Bucket executePlan(Plan plan, PlannerContext plannerContext) throws Exception {
        return executePlan(plan, plannerContext, Row.EMPTY);
    }
}
