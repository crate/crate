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

package io.crate.import_;

import com.google.common.collect.ImmutableList;
import io.crate.Id;
import org.elasticsearch.action.index.IndexRequest;
import org.junit.Test;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.*;

public class ImporterTest {

    @Test
    public void testParseId() throws Exception {
        IndexRequest r = Importer.parseObject("{\"_id\":\"i\"}", "test",
                "default", Importer.DEFAULT_PRIMARY_KEYS, null);
        assertEquals("i", r.id());
    }


    @Test
    public void testParseIdAfterSubObject() throws Exception {
        IndexRequest r = Importer.parseObject("{\"o\":{\"x\":1},\"_id\":\"i\"}", "test",
                "default", Importer.DEFAULT_PRIMARY_KEYS, null);
        assertEquals("i", r.id());
    }

    @Test
    public void testParsePrimaryKey() throws Exception {
        IndexRequest r = Importer.parseObject("{\"o\":{\"x\":1},\"mykey\":\"i\"}", "test",
                "default", ImmutableList.of("mykey"), null);
        assertEquals("i", r.id());
    }

    @Test
    public void testPreferIdOverPrimaryKey() throws Exception {
        IndexRequest r = Importer.parseObject("{\"mykey\":\"k\",\"_id\":\"i\"}", "test",
                "default", ImmutableList.of("mykey"), null);
        assertEquals("i", r.id());
        r = Importer.parseObject("{\"_id\":\"i\",\"mykey\":\"k\"}", "test", "default", ImmutableList.of("mykey"), null);
        assertEquals("i", r.id());
    }

    @Test
    public void testNestedIdNotPrimaryKey() throws Exception {
        String src = "{\"contributor\":{\"username\":\"Gtrmp\",\"id\":\"sub\"}," +
                "\"id\":\"top\"}";
        IndexRequest r = Importer.parseObject(src, "test", "default", ImmutableList.of("id"), null);
        assertEquals("top", r.id());
    }

    @Test
    public void testParseRoutingValue() throws Exception {
        IndexRequest r = Importer.parseObject("{\"o\":{\"x\":1},\"mykey\":\"i\"}", "test",
                "default", Importer.DEFAULT_PRIMARY_KEYS, "mykey");
        assertEquals("i", r.routing());
    }

    @Test
    public void testPreferRoutingOverRoutingValue() throws Exception {
        IndexRequest r = Importer.parseObject("{\"mykey\":\"k\",\"_routing\":\"i\"}", "test",
                "default", Importer.DEFAULT_PRIMARY_KEYS, "mkey");
        assertEquals("i", r.routing());
        r = Importer.parseObject("{\"_routing\":\"i\",\"mykey\":\"k\"}", "test", "default", Importer.DEFAULT_PRIMARY_KEYS, "mykey");
        assertEquals("i", r.routing());
    }

    @Test
    public void testGenerateIdWithRouting() throws Exception {
        IndexRequest r = Importer.parseObject("{\"o\":{\"x\":1},\"mykey\":\"i\"}", "test",
                "default", Importer.DEFAULT_PRIMARY_KEYS, "mykey");

        // Validate generated _id, must be: <generatedRandom>
        String _id = r.id();
        assertNotNull(_id);
        assertThat(_id.length(), greaterThan(0));
    }

    @Test
    public void testGenerateIdWithRoutingAndPks() throws Exception {
        IndexRequest r = Importer.parseObject("{\"mykey\":\"i\", \"mykey2\":\"b\"}", "test",
                "default", ImmutableList.of("mykey", "mykey2"), "mykey2");

        // Validate generated _id, must be a base64 encoded string of: b:i
        String _id = r.id();
        assertNotNull(_id);
        assertEquals(ImmutableList.of("b", "i"), Id.fromString(_id).values());
    }

}
