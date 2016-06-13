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

import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLBulkResponse;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.*;

public class CrateClientUsageTest extends CrateClientIntegrationTest {

    @Before
    public void prepare() {
        client = new CrateClient(serverAddress());
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            execute("drop table if exists test");
            client.close();
            client = null;
        }
    }

    @Test
    public void testCreateTable() throws Exception {
        execute("create table test (id int) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test (id) values (1)");
        execute("refresh table test");
        SQLResponse r = execute("select id from test");

        assertEquals(1, r.rows().length);
        assertEquals("id", r.cols()[0]);
        assertEquals(1, r.rows()[0][0]);

        assertThat(r.columnTypes(), is(new DataType[] {DataTypes.INTEGER }));
    }

    @Test
    public void testTryCreateTransportFor() throws Exception {

        // These host/port combination should work
        assertThat(client().tryCreateTransportFor("localhost:1234"), instanceOf(InetSocketTransportAddress.class));
        assertThat(client().tryCreateTransportFor("1.2.3.4:1234"), instanceOf(InetSocketTransportAddress.class));
        assertThat(client().tryCreateTransportFor("www.crate.io:1234"), instanceOf(InetSocketTransportAddress.class));
        assertThat(client().tryCreateTransportFor("crate.io"), instanceOf(InetSocketTransportAddress.class));
        assertThat(client().tryCreateTransportFor("http://crate.io:4321"), instanceOf(InetSocketTransportAddress.class));


        // IPV6 - should not throw exceptions
        assertThat(client().tryCreateTransportFor("[2001:4860:4860::8888]"), instanceOf(InetSocketTransportAddress.class));
        assertThat(client().tryCreateTransportFor("[::1]:1234"), instanceOf(InetSocketTransportAddress.class));
        assertThat(client().tryCreateTransportFor("[FEDC:BA98:7654:3210:FEDC:BA98:7654:3210]:80"), instanceOf(InetSocketTransportAddress.class));
        assertThat(client().tryCreateTransportFor("[1080::8:800:200C:417A]"), instanceOf(InetSocketTransportAddress.class));

        // no brackets = no IPv6 literal
        assertNull(client().tryCreateTransportFor("1080::8:800:200C:417A"));
    }


    @Test
    public void testAsyncRequest() throws Throwable {
        execute("create table test (id int) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test (id) values (1)");
        execute("refresh table test");

        // In practice use ActionListener onResponse and onFailure to create a Promise instead
        final SettableFuture<Boolean> future = SettableFuture.create();
        final AtomicReference<Throwable> assertionError = new AtomicReference<>();

        ActionListener<SQLResponse> listener = new ActionListener<SQLResponse>() {
            @Override
            public void onResponse(SQLResponse r) {
                try {
                    assertEquals(1, r.rows().length);
                    assertEquals("id", r.cols()[0]);
                    assertEquals(1, r.rows()[0][0]);

                    assertThat(r.columnTypes(), is(new DataType[] { DataTypes.INTEGER }));
                } catch (AssertionError e) {
                    assertionError.set(e);
                } finally {
                    future.set(true);
                }

            }
            @Override
            public void onFailure(Throwable e) {
                future.set(true);
                assertionError.set(e);
            }
        };
        client.sql("select id from test", listener);

        // this will block until timeout is thrown if listener is not called
        assertThat(future.get(SQL_REQUEST_TIMEOUT.getSeconds(), TimeUnit.SECONDS), is(true));
        Throwable error = assertionError.get();
        if (error != null) {
            throw error;
        }
    }

    @Test
    public void testRequestWithTypes() throws Exception {
        execute("create table test (id int) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test (id) values (1)");
        execute("refresh table test");

        SQLRequest request =  new SQLRequest("select id from test");
        request.includeTypesOnResponse(true);
        SQLResponse r = execute(request);

        assertEquals(1, r.rows().length);
        assertEquals("id", r.cols()[0]);
        assertEquals(1, r.rows()[0][0]);

        assertThat(r.columnTypes()[0], instanceOf(IntegerType.class));
    }

    @Test
    public void testSetSerialization() throws Exception {
        SQLResponse r = execute("select constraint_name from information_schema.table_constraints");
        assertTrue(r.rows()[0][0] instanceof Object[]);
        assertThat(((Object[]) r.rows()[0][0])[0], instanceOf(String.class));
    }

    @Test
    public void testBulkSql() throws Exception {
        execute("create table test (a string, b int) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into test (a, b) values ('foo', 1)");
        execute("refresh table test");

        SQLBulkResponse bulkResponse = execute("update test set a = ? where b = ?",
                new Object[][]{new Object[]{"bar", 1}, new Object[]{"baz", 1}});
        assertThat(bulkResponse.results().length, is(2));
        for (SQLBulkResponse.Result result : bulkResponse.results()) {
            assertThat(result.rowCount(), is(1L));
            assertThat(result.errorMessage(), is(nullValue()));
        }
    }

    @Test
    public void testClusterSettingsSerialization() throws Exception {
        SQLResponse r = execute("select settings from sys.cluster");
        assertThat(r.rowCount(), is(1L));
        assertTrue(r.rows()[0][0] instanceof Map);
    }

    @Test
    public void testException() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("line 1:1: no viable alternative at input 'error'");
        execute("error");
    }
}
