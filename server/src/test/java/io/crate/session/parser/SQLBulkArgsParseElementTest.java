/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.session.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.util.List;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;


public class SQLBulkArgsParseElementTest extends ESTestCase {

    private List<List<Object>> parse(String bulkArgs) throws Exception {
        SQLRequestParseContext context = new SQLRequestParseContext();
        String json = "{\"bulk_args\":" + bulkArgs + "}";
        XContentParser parser = XContentType.JSON.xContent().createParser(
            xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();
        SQLBulkArgsParseElement bulkArgsParseElement = new SQLBulkArgsParseElement();
        bulkArgsParseElement.parse(parser, context);
        return context.bulkArgs();
    }

    @Test
    public void testBulkArgsArray() throws Exception {
        String bulkArgs = "[[\"200\", \"Somewhere\", \"planet\"], [\"201\", \"Somewhere else\", \"city\"]]";
        assertThat(parse(bulkArgs)).containsExactly(
                List.of("200", "Somewhere", "planet"),
                List.of("201", "Somewhere else", "city"));
    }

    @Test
    public void testEmptyBulkArgsArray() throws Exception {
        assertThat(parse("[]").isEmpty()).isTrue();
    }

    @Test
    public void testInvalidBulkArgsArray() throws Exception {
        assertThatThrownBy(() -> parse("[[\"hello\"], null]"))
            .isExactlyInstanceOf(SQLParseSourceException.class)
            .hasMessage("Parse Failure [Field [null] has an invalid value]");
    }
}
