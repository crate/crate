/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package org.elasticsearch.common.xcontent.support;

import static org.elasticsearch.common.xcontent.support.AbstractXContentParser.readValue;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.LinkedHashMap;

public class ParameterizedXContentParser {

    public interface FieldParser {
        Object parse(String field, Object value);
    }

    /**
     * Replication of {@link AbstractXContentParser#readMap(XContentParser, AbstractXContentParser.MapFactory)}
     * with an additional field parsing before inserting into the resulting map.
     */
    public static LinkedHashMap<String, Object> parse(XContentParser parser, FieldParser fieldParser) throws IOException {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
        }
        for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
            // Must point to field name
            String fieldName = parser.currentName();
            // And then the value...
            token = parser.nextToken();
            Object value = readValue(parser, LinkedHashMap::new, token);
            map.put(fieldName, fieldParser.parse(fieldName, value));
        }
        return map;
    }
}
