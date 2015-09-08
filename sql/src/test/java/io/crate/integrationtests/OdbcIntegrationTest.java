/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.integrationtests;

import io.crate.action.sql.SQLAction;
import io.crate.action.sql.SQLBaseRequest;
import io.crate.action.sql.SQLRequestBuilder;
import io.crate.action.sql.SQLResponse;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class OdbcIntegrationTest extends SQLTransportIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private Setup setup = new Setup(sqlExecutor);

    private SQLResponse execute(SQLRequestBuilder requestBuilder) {
        response = client().execute(SQLAction.INSTANCE, requestBuilder.request()).actionGet();
        return response;
    }

    private SQLRequestBuilder quotedRequest(String stmt) {
        SQLRequestBuilder requestBuilder = new SQLRequestBuilder(client());
        requestBuilder.stmt(stmt);
        // Set Odbc flag
        requestBuilder.addFlagsToRequestHeader(SQLBaseRequest.HEADER_FLAG_ALLOW_QUOTED_SUBSCRIPT);
        return requestBuilder;
    }

    @Before
    public void initTestData() {
        this.setup.setUpObjectTable();
        ensureYellow();
    }

    @Test
    public void testSelectDynamicQuotedObjectLiteral() throws Exception {
        execute(quotedRequest("select \"author['name']['first_name']\", \"author['name']['last_name']\" " +
                "from ot"));
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testSelectDynamicQuotedObjectLiteralWithTableAlias() throws Exception {
        execute(quotedRequest("select \"authors\".\"author['name']['first_name']\", " +
                                    "\"authors\".\"author['name']['last_name']\" " +
                                "from \"ot\" \"authors\""));
        assertEquals(1L, response.rowCount());
    }
}
