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
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.cratedb.module.reindex.test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import org.cratedb.action.reindex.ReindexAction;
import org.cratedb.action.searchinto.SearchIntoRequest;
import org.cratedb.action.searchinto.SearchIntoResponse;
import org.cratedb.module.AbstractRestActionTest;

public class RestReindexActionTest extends AbstractRestActionTest {

    @Override
    public Settings indexSettings() {
        return ImmutableSettings.builder().put("number_of_shards", 1).build();
    }

    @Test
    public void testSearchIntoWithoutSource() {
        prepareCreate("test").addMapping("a",
            "{\"a\":{\"_source\": {\"enabled\": false}}}").execute().actionGet();
        client().index(new IndexRequest("test", "a", "1").source("{\"name\": \"John\"}")).actionGet();
        SearchIntoRequest request = new SearchIntoRequest("test");
        SearchIntoResponse res = client().execute(ReindexAction.INSTANCE, request).actionGet();
        assertEquals(1, res.getFailedShards());
        assertTrue(res.getShardFailures()[0].reason().contains("Parse Failure [The _source field of index test and type a is not stored.]"));
    }
}
