/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.action.sql.SQLResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

public class MappingDefaultsTest extends SQLTransportIntegrationTest {

    @Test
    public void testAllFieldsDisabled() throws Exception {
        execute("create table test (col1 string)");
        ensureYellow();

        SQLResponse sqlResponse = execute("insert into test (col1) values ('foo')");
        assertEquals(1, sqlResponse.rowCount());
        refresh();


        byte[] searchSource = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("query")
                    .startObject("query_string")
                        .field("query", "foo")
                    .endObject()
                .endObject()
                .endObject().bytes().toBytes();

        SearchRequest searchRequest = new SearchRequest("test");
        searchRequest.source(searchSource);
        SearchResponse response = client().search(searchRequest).actionGet();
        assertEquals(0L, response.getHits().totalHits());
    }
}
