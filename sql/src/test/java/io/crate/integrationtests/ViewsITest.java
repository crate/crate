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

package io.crate.integrationtests;

import io.crate.metadata.view.ViewsMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class ViewsITest extends SQLTransportIntegrationTest {

    @Test
    public void testViewCanBeCreatedAndThenDropped() throws Exception {
        execute("create view v1 as select 1");
        for (ClusterService clusterService : internalCluster().getInstances(ClusterService.class)) {
            ViewsMetaData views = clusterService.state().metaData().custom(ViewsMetaData.TYPE);
            assertThat(views, Matchers.notNullValue());
            assertThat(views.contains(sqlExecutor.getDefaultSchema() + ".v1"), is(true));
        }
        execute("drop view v1");
        for (ClusterService clusterService : internalCluster().getInstances(ClusterService.class)) {
            ViewsMetaData views = clusterService.state().metaData().custom(ViewsMetaData.TYPE);
            assertThat(views.contains(sqlExecutor.getDefaultSchema() + ".v1"), is(false));
        }
    }

    @Test
    public void testViewCanBeCreatedAndThenReplaced() throws Exception {
        execute("create view v2 as select 1");
        execute("create or replace view v2 as select 1");
        for (ClusterService clusterService : internalCluster().getInstances(ClusterService.class)) {
            ViewsMetaData views = clusterService.state().metaData().custom(ViewsMetaData.TYPE);
            assertThat(views, Matchers.notNullValue());
            assertThat(views.contains(sqlExecutor.getDefaultSchema() + ".v2"), is(true));
        }
    }

    @Test
    public void testCreateViewFailsIfViewAlreadyExists() {
        execute("create view v3 as select 1");

        expectedException.expectMessage("Relation '" + sqlExecutor.getDefaultSchema() + ".v3' already exists");
        execute("create view v3 as select 1");
    }

    @Test
    public void testCreateViewFailsIfNameConflictsWithTable() {
        execute("create table t1 (x int) clustered into 1 shards with (number_of_replicas = 0)");

        expectedException.expectMessage("Relation '" + sqlExecutor.getDefaultSchema() + ".t1' already exists");
        execute("create view t1 as select 1");
    }

    @Test
    public void testDropViewFailsIfViewIsMissing() {
        expectedException.expectMessage("Relations not found: " + sqlExecutor.getDefaultSchema() + ".v1");
        execute("drop view v1");
    }

    @Test
    public void testDropViewDoesNotFailIfViewIsMissingAndIfExistsIsUsed() {
        execute("drop view if exists v1");
    }
}
