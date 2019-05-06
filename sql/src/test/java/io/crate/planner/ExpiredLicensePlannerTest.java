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

package io.crate.planner;

import io.crate.exceptions.LicenseViolationException;
import io.crate.metadata.RelationName;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.join.Join;
import io.crate.planner.statement.SetLicensePlan;
import io.crate.planner.statement.SetSessionPlan;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static io.crate.analyze.TableDefinitions.USER_TABLE_DEFINITION;
import static org.hamcrest.Matchers.instanceOf;

public class ExpiredLicensePlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .addTable(USER_TABLE_DEFINITION)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .addView(new RelationName("doc", "v1"), "select * from users")
            .setHasValidLicense(false)
            .build();
    }

    @Test
    public void testSelectPlanOnUserSchemaThrowsException() {
        expectedException.expect(LicenseViolationException.class);
        expectedException.expectMessage("Statement not allowed");
        e.plan("select id from users where id = 1");
    }

    @Test
    public void testSelectPlanOnInformationSchemaIsAllowed() {
        Collect plan = e.plan("select * from information_schema.tables");
        assertThat(plan, instanceOf(Collect.class));
    }

    @Test
    public void testSelectPlanOnSysSchemaIsAllowed() {
        Collect plan = e.plan("select * from sys.cluster");
        assertThat(plan, instanceOf(Collect.class));
    }

    @Test
    public void testUnionSelectPlanOnSysSchemaIsAllowed() {
        UnionExecutionPlan plan = e.plan("select * from sys.cluster where id = 1 " +
                                         "union all " +
                                         "select * from sys.cluster where id = 2");
        assertThat(plan, instanceOf(UnionExecutionPlan.class));
    }

    @Test
    public void testUnionSelectPlanOnUserSchemaThrowsException() {
        expectedException.expect(LicenseViolationException.class);
        expectedException.expectMessage("Statement not allowed");
        e.plan("select * from users where id = 1 " +
               "union all " +
               "select * from users where id = 2");
    }

    @Test
    public void testUnionSelectPlanOnMixedSchemaThrowsException() {
        expectedException.expect(LicenseViolationException.class);
        expectedException.expectMessage("Statement not allowed");
        e.plan("select name from sys.cluster " +
               "union all " +
               "select name from doc.users");
    }

    @Test
    public void testOrderedLimitedPlanOnSysSchemaIsAllowed() {
        UnionExecutionPlan plan = e.plan("select * from sys.cluster where id = 1 " +
                                         "union all " +
                                         "select * from sys.cluster where id = 2 " +
                                         "order by 1");
        assertThat(plan, instanceOf(UnionExecutionPlan.class));
    }

    @Test
    public void testOrderedLimitedPlanOUserSchemaThrowsException() {
        expectedException.expect(LicenseViolationException.class);
        expectedException.expectMessage("Statement not allowed");
        e.plan("select * from users where id = 1 " +
               "union all " +
               "select * from users where id = 2 " +
               "order by id");
    }

    @Test
    public void testOrderedLimitedPlanOMixedSchemaThrowsException() {
        expectedException.expect(LicenseViolationException.class);
        expectedException.expectMessage("Statement not allowed");
        e.plan("select name from sys.cluster where id = 1 " +
               "union all " +
               "select name from users where id = 2 " +
               "order by name");
    }

    @Test
    public void testMultiSourceSelectPlanOnSysSchemaIsAllowed() {
        Join plan = e.plan("select cl.id, sh.id from sys.cluster cl, sys.shards sh where cl.id = 1");
        assertThat(plan, instanceOf(Join.class));
    }

    @Test
    public void testMultiSourceSelectPlanOUserSchemaThrowsException() {
        expectedException.expect(LicenseViolationException.class);
        expectedException.expectMessage("Statement not allowed");
        e.plan("select t1.x, t2.y from t1, t2 where t1.x = 10");
    }

    @Test
    public void testMultiSourceSelectPlanOnMixedSchemaThrowsException() {
        expectedException.expect(LicenseViolationException.class);
        expectedException.expectMessage("Statement not allowed");
        e.plan("select t1.x, sh.id from t1, sys.shards sh where t1.x = 10");
    }

    @Test
    public void testInsertPlanThrowsException() {
        expectedException.expect(LicenseViolationException.class);
        expectedException.expectMessage("Statement not allowed");
        e.plan("insert into users (id, name) values (42, 'Deep Thought')");
    }

    @Test
    public void testUpdatePlanThrowsException() {
        expectedException.expect(LicenseViolationException.class);
        expectedException.expectMessage("Statement not allowed");
        e.plan("update users set name='Vogon lyric fan' where id = 1");
    }

    @Test
    public void testDeletePlanThrowsException() {
        expectedException.expect(LicenseViolationException.class);
        expectedException.expectMessage("Statement not allowed");
        e.plan("delete from users where id = 1");
    }

    @Test
    public void testExplainPlanThrowsException() {
        expectedException.expect(LicenseViolationException.class);
        expectedException.expectMessage("Statement not allowed");
        e.plan("explain analyze select id from users where id = 1");
    }

    @Test
    public void testCreateTablePlanThrowsException() {
        expectedException.expect(LicenseViolationException.class);
        expectedException.expectMessage("Statement not allowed");
        e.plan("create table users2(name string)");
    }

    @Test
    public void testDropTablePlanThrowsException() {
        expectedException.expect(LicenseViolationException.class);
        expectedException.expectMessage("Statement not allowed");
        e.plan("drop table users");
    }

    @Test
    public void testCopyPlanThrowsException() {
        expectedException.expect(LicenseViolationException.class);
        expectedException.expectMessage("Statement not allowed");
        e.plan("copy users (name) to directory '/tmp'");
    }

    @Test
    public void testCreateViewThrowsException() {
        expectedException.expect(LicenseViolationException.class);
        expectedException.expectMessage("Statement not allowed");
        e.plan("create view v2 as select * from users");
    }

    @Test
    public void testDropViewThrowsException() {
        expectedException.expect(LicenseViolationException.class);
        expectedException.expectMessage("Statement not allowed");
        e.plan("drop view v1");
    }

    @Test
    public void testSetGlobalThrowsException() {
        expectedException.expect(LicenseViolationException.class);
        expectedException.expectMessage("Statement not allowed");
        e.plan("set global transient stats.enabled=false,stats.jobs_log_size=0");
    }

    @Test
    public void testSetLicenseIsAllowed() {
        Plan plan = e.plan("set license 'XXX'");
        assertThat(plan, instanceOf(SetLicensePlan.class));
    }

    @Test
    public void testDecommissionNodeIsAllowed() {
        Plan plan = e.plan("alter cluster decommission 'XXX'");
        assertThat(plan, instanceOf(DecommissionNodePlan.class));
    }

    @Test
    public void testQueryOnTableFunctionIsAllowedIfLicenseIsExpired() {
        ExecutionPlan plan = e.plan("select null as \"user\", current_schema as \"schema\"");
        assertThat(plan, instanceOf(Collect.class));
    }

    @Test
    public void testSetSessionStatementIsAllowedIfLicenseIsExpired() {
        // postgres clients send set session statements initially on connecting, we want to allow them to.
        Plan plan = e.plan("set session whatever = 'x'");
        assertThat(plan, instanceOf(SetSessionPlan.class));
    }
}
