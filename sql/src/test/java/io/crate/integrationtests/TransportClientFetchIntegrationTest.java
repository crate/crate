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

import io.crate.action.sql.FetchProperties;
import io.crate.action.sql.SQLAction;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.action.sql.fetch.FetchRequest;
import io.crate.action.sql.fetch.FetchResponse;
import io.crate.action.sql.fetch.SQLFetchAction;
import org.elasticsearch.client.Client;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;

public class TransportClientFetchIntegrationTest extends SQLTransportIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testFetch() throws Exception {
        execute("create table t (x int)");
        execute("insert into t (x) values (?)", new Object[][] {
            new Object[] { 1 },
            new Object[] { 2 },
            new Object[] { 3 },
            new Object[] { 4 },
            new Object[] { 5 },
            new Object[] { 6 },
        });
        execute("refresh table t");

        Client client = client();
        // avoid fetch projection for now - it doesn't support pause/resume
        SQLRequest request = new SQLRequest("select x from t order by x");
        request.setFetchProperties(new FetchProperties(2, false));
        SQLResponse resp = client.execute(SQLAction.INSTANCE, request).actionGet(5, TimeUnit.SECONDS);
        assertThat(resp.rowCount(), is(2L));

        FetchRequest fetchRequest = new FetchRequest(resp.cursorId(), new FetchProperties(2, false));
        FetchResponse fetchResponse = client.execute(SQLFetchAction.INSTANCE, fetchRequest).actionGet(5, TimeUnit.SECONDS);
        assertThat(fetchResponse.rows().length, is(2));

        fetchRequest = new FetchRequest(resp.cursorId(), new FetchProperties(2, true));
        fetchResponse = client.execute(SQLFetchAction.INSTANCE, fetchRequest).actionGet(5, TimeUnit.SECONDS);
        assertThat(fetchResponse.rows().length, is(2));

        expectedException.expectMessage("No context for cursorId");
        client.execute(SQLFetchAction.INSTANCE, fetchRequest).actionGet(5, TimeUnit.SECONDS);
    }
}
