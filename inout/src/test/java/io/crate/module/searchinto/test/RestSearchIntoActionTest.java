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

package io.crate.module.searchinto.test;

import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.junit.Test;

import io.crate.action.searchinto.SearchIntoAction;
import io.crate.action.searchinto.SearchIntoRequest;
import io.crate.action.searchinto.SearchIntoResponse;
import io.crate.module.AbstractRestActionTest;

public class RestSearchIntoActionTest extends AbstractRestActionTest {

    @Test
    public void testSearchIntoWithoutSource() {

        prepareCreate("test").addMapping("a", "{\"a\":{\"_source\": {\"enabled\": false}}}").execute().actionGet();
        client().index(new IndexRequest("test", "a", "1").source("{\"name\": \"John\"}")).actionGet();
        SearchIntoRequest request = new SearchIntoRequest("test");
        request.source("{\"fields\": [\"_id\", \"_source\", [\"_index\", \"'newindex'\"]]}");
        SearchIntoResponse res = client().execute(SearchIntoAction.INSTANCE, request).actionGet();
        assertEquals(1, res.getFailedShards());

        assertTrue(res.getShardFailures()[0].reason().contains("Parse Failure [The _source field of index test and type a is not stored.]"));
    }

    @Test
    public void testNestedObjectsRewriting() {
        prepareNested();
        SearchIntoRequest request = new SearchIntoRequest("test");
        request.source("{\"fields\": [\"_id\", [\"x.city\", \"_source.city\"], [\"x.surname\", \"_source.name.surname\"], [\"x.name\", \"_source.name.name\"], [\"_index\", \"'newindex'\"]]}");
        SearchIntoResponse res = client().execute(SearchIntoAction.INSTANCE, request).actionGet();

        GetRequestBuilder rb = new GetRequestBuilder(client(), "newindex");
        GetResponse getRes = rb.setType("a").setId("1").execute().actionGet();
        assertTrue(getRes.isExists());
        assertEquals("{\"x\":{\"name\":\"Doe\",\"surname\":\"John\",\"city\":\"Dornbirn\"}}", getRes.getSourceAsString());
    }

    @Test
    public void testNestedObjectsRewritingMixed1() {
        prepareNested();
        SearchIntoRequest request = new SearchIntoRequest("test");
        request.source("{\"fields\": [\"_id\", [\"x\", \"_source.city\"], [\"x.surname\", \"_source.name.surname\"], [\"x.name\", \"_source.name.name\"], [\"_index\", \"'newindex'\"]]}");
        SearchIntoResponse res = client().execute(SearchIntoAction.INSTANCE, request).actionGet();
        assertTrue(res.getShardFailures()[0].reason().contains("Error on rewriting objects: Mixed objects and values]"));
    }

    @Test
    public void testNestedObjectsRewritingMixed2() {
        prepareNested();
        SearchIntoRequest request = new SearchIntoRequest("test");
        request.source("{\"fields\": [\"_id\", [\"x.surname.bad\", \"_source.city\"], [\"x.surname\", \"_source.name.surname\"], [\"x.name\", \"_source.name.name\"], [\"_index\", \"'newindex'\"]]}");
        SearchIntoResponse res = client().execute(SearchIntoAction.INSTANCE, request).actionGet();
        assertTrue(res.getShardFailures()[0].reason().contains("Error on rewriting objects: Mixed objects and values]"));
    }

    @Test
    public void testNestedObjectsRewritingMixed3() {
        prepareNested();
        SearchIntoRequest request = new SearchIntoRequest("test");
        request.source("{\"fields\": [\"_id\", [\"x.surname\", \"_source.city\"], [\"x.surname.bad\", \"_source.name.surname\"], [\"x.name\", \"_source.name.name\"], [\"_index\", \"'newindex'\"]]}");
        SearchIntoResponse res = client().execute(SearchIntoAction.INSTANCE, request).actionGet();
        assertTrue(res.getShardFailures()[0].reason().contains("Error on rewriting objects: Mixed objects and values]"));
    }


    private void prepareNested() {
        prepareCreate("test").addMapping("a",
            "{\"a\": {\"properties\": {\"name\": {\"properties\": {\"surname\":{\"type\":\"string\"}, \"name\": {\"type\":\"string\"}}}, \"city\": {\"type\": \"string\"}}}}")
            .execute().actionGet();

        client().index(new IndexRequest("test", "a", "1").source(
            "{\"city\": \"Dornbirn\", \"name\": {\"surname\": \"John\", \"name\": \"Doe\"}}")).actionGet();
        refresh();
    }
}
