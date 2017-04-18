/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.action.sql.parser;


import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

public class SQLBulkArgsParseElementTest extends CrateUnitTest {

    private Object[][] parse(String bulk_args) throws Exception {
        SQLXContentSourceContext context = new SQLXContentSourceContext();
        String json = "{\"bulk_args\":" + bulk_args + "}";
        BytesArray bytes = new BytesArray(json);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(NamedXContentRegistry.EMPTY, bytes);
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();
        SQLBulkArgsParseElement bulkArgsParseElement = new SQLBulkArgsParseElement();
        bulkArgsParseElement.parse(parser, context);
        return context.bulkArgs();

    }

    @Test
    public void testBulkArgsArray() throws Exception {
        String bulk_args = "[[\"200\", \"Somewhere\", \"planet\"], [\"201\", \"Somewhere else\", \"city\"]]";
        Object[][] bulk_array = parse(bulk_args);
        assertArrayEquals(new Object[]{new Object[]{"200", "Somewhere", "planet"},
            new Object[]{"201", "Somewhere else", "city"}}, bulk_array);
    }

    @Test
    public void testEmptyBulkArgsArray() throws Exception {
        String bulk_args = "[]";
        Object[][] bulk_array = parse(bulk_args);
        assertEquals(0, bulk_array.length);
    }

    @Test
    public void testInvalidBulkArgsArray() throws Exception {
        String bulk_args = "[[\"hello\"], null]";
        try {
            parse(bulk_args);
        } catch (SQLParseSourceException e) {
            assertEquals("Parse Failure [Field [null] has an invalid value]", e.getMessage());
        }
    }
}
