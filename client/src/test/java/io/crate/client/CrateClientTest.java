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

package io.crate.client;

import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.types.DataType;
import io.crate.types.StringType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class CrateClientTest extends CrateIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private CrateClient client;

    @Before
    public void prepare() {
        int port = ((InetSocketTransportAddress) cluster()
                .getInstance(TransportService.class)
                .boundAddress().boundAddress()).address().getPort();
        client = new CrateClient("localhost:" + port);
    }

    @Test
    public void testCreateClient() throws Exception {
        client().prepareIndex("test", "default", "1")
            .setRefresh(true)
            .setSource("{}")
            .execute()
            .actionGet();

        SQLResponse r = client.sql("select \"_id\" from test").actionGet();

        assertEquals(1, r.rows().length);
        assertEquals("_id", r.cols()[0]);
        assertEquals("1", r.rows()[0][0]);

        assertThat(r.columnTypes(), is(new DataType[0]));

        System.out.println(Arrays.toString(r.cols()));
        for (Object[] row: r.rows()){
            System.out.println(Arrays.toString(row));
        }

    }

    @Test
    public void testRequestWithTypes() throws Exception {
        client().prepareIndex("test", "default", "1")
            .setRefresh(true)
            .setSource("{}")
            .execute()
            .actionGet();

        SQLRequest request =  new SQLRequest("select \"_id\" from test");
        request.includeTypesOnResponse(true);
        SQLResponse r = client.sql(request).actionGet();

        assertEquals(1, r.rows().length);
        assertEquals("_id", r.cols()[0]);
        assertEquals("1", r.rows()[0][0]);

        assertThat(r.columnTypes()[0], instanceOf(StringType.class));

        System.out.println(Arrays.toString(r.cols()));
        for (Object[] row: r.rows()){
            System.out.println(Arrays.toString(row));
        }

    }

    @Test
    public void testSetSerialization() throws Exception {
        SQLResponse r = client.sql("select constraint_name " +
                "from information_schema.table_constraints").actionGet();
        assertTrue(r.rows()[0][0] instanceof Object[]);
        assertThat(((Object[]) r.rows()[0][0])[0], instanceOf(String.class));
    }

    @Test
    public void testSettings() throws Exception {
        Settings settings = client.settings();

        assertEquals(false, settings.getAsBoolean("network.server", true));
        assertEquals(true, settings.getAsBoolean("node.client", false));
        assertEquals(true, settings.getAsBoolean("client.transport.ignore_cluster_name", false));
        assertThat(settings.get("node.name"), startsWith("crate-client-"));
    }

}
