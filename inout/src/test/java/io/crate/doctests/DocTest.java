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

package io.crate.doctests;

import io.crate.rest.CrateRestFilter;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.test.integration.DoctestTestCase;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.crate.test.integration.PathAccessor.stringFromPath;


@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.SUITE, numNodes = 0)
public class DocTest extends DoctestTestCase {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    String node1;
    String node2;


    @Before
    public void setUpNodes() throws Exception {

        Settings s1 = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "a")
                .put("http.port", 44202)
                .put("transport.tcp.port", 44302)
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(CrateRestFilter.ES_API_ENABLED_SETTING, true)
                .build();

        node1 = cluster().startNode(s1);
        client(node1).admin().indices().prepareCreate("users").setSettings(
            ImmutableSettings.builder().loadFromClasspath("essetup/settings/test_b.json").build())
            .addMapping("d", stringFromPath("/essetup/mappings/test_b.json", getClass())).execute().actionGet();

        loadBulk(client(node1), "/essetup/data/test_b.json", getClass());
        client(node1).admin().indices().prepareRefresh("users").execute().actionGet();

        Settings s2 = ImmutableSettings.settingsBuilder()
                .put("http.port", 44203)
                .put("transport.tcp.port", 44303)
                .put("cluster.name", "b")
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(CrateRestFilter.ES_API_ENABLED_SETTING, true)
                .build();
        node2 = cluster().startNode(s2);

        waitForRelocation(ClusterHealthStatus.GREEN);
    }

    @After
    public void tearDownNodes() throws Exception {
        client(node1).admin().indices().prepareDelete("users").execute().actionGet();
        cluster().stopNode(node1);
        cluster().stopNode(node2);
        super.after();
    }

    @Test
    public void testSearchInto() throws Exception {
        execDocFile("search_into.rst", getClass());
    }

    @Test
    public void testReindex() throws Exception {
        execDocFile("reindex.rst", getClass());
    }

}
