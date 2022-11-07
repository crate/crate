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

package io.crate.expression.reference.doc.lucene;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Map;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.common.collections.Maps;
import io.crate.metadata.ColumnIdent;
import io.crate.sql.tree.BitString;
import io.crate.types.BitStringType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

public class SourceParserTest extends ESTestCase {

    @Test
    public void test_extract_single_value_from_json_with_multiple_columns() throws Exception {
        SourceParser sourceParser = new SourceParser();
        var column = new ColumnIdent("_doc", List.of("x"));
        sourceParser.register(column, DataTypes.INTEGER);
        Map<String, Object> result = sourceParser.parse(new BytesArray(
            """
               {"x": 10, "y": 20}
            """));

        assertThat(result.get("x"), is(10));
        assertThat(result.get("y"), Matchers.nullValue());
    }

    @Test
    public void test_unnecessary_leafs_of_object_columns_are_not_collected() throws Exception {
        SourceParser sourceParser = new SourceParser();
        var x = new ColumnIdent("_doc", List.of("obj", "x"));
        var z = new ColumnIdent("_doc", List.of("obj", "z"));
        sourceParser.register(x, DataTypes.INTEGER);
        sourceParser.register(z, DataTypes.LONG);
        Map<String, Object> result = sourceParser.parse(new BytesArray(
            """
                {"obj": {"x": 1, "y": 2, "z": 3}}
            """));

        assertThat(Maps.getByPath(result, "obj.x"), is(1));
        assertThat(Maps.getByPath(result, "obj.y"), Matchers.nullValue());
        assertThat(Maps.getByPath(result, "obj.z"), is(3L));
    }

    @Test
    public void test_full_object_is_collected_if_full_object_requested() throws Exception {
        SourceParser sourceParser = new SourceParser();
        var obj = new ColumnIdent("_doc", List.of("obj"));
        var x = new ColumnIdent("_doc", List.of("obj", "x"));
        // the order in which the columns are registered must not matter
        boolean xFirst = randomBoolean();
        if (xFirst) {
            sourceParser.register(x, DataTypes.INTEGER);
            sourceParser.register(obj, ObjectType.UNTYPED);
        } else {
            sourceParser.register(obj, ObjectType.UNTYPED);
            sourceParser.register(x, DataTypes.INTEGER);
        }

        Map<String, Object> result = sourceParser.parse(new BytesArray(
            """
                {"obj": {"x": 1, "y": 2}}
            """));

        assertThat(result.get("obj"), is(Map.of("x", 1, "y", 2)));
    }

    @Test
    public void test_string_encoded_numbers_will_be_parsed_by_data_type() {
        SourceParser sourceParser = new SourceParser();
        sourceParser.register(new ColumnIdent("_doc", List.of("i")), DataTypes.INTEGER);
        sourceParser.register(new ColumnIdent("_doc", List.of("l")), DataTypes.LONG);
        sourceParser.register(new ColumnIdent("_doc", List.of("f")), DataTypes.FLOAT);
        sourceParser.register(new ColumnIdent("_doc", List.of("d")), DataTypes.DOUBLE);
        sourceParser.register(new ColumnIdent("_doc", List.of("s")), DataTypes.SHORT);
        sourceParser.register(new ColumnIdent("_doc", List.of("b")), DataTypes.BYTE);
        sourceParser.register(new ColumnIdent("_doc", List.of("ts")), DataTypes.TIMESTAMP);
        sourceParser.register(new ColumnIdent("_doc", List.of("tsz")), DataTypes.TIMESTAMPZ);
        Map<String, Object> result = sourceParser.parse(new BytesArray(
            """
            {"i": "1", "l": "2", "f": "0.12", "d": "0.23", "s": "4", "b": "5", "ts": "915757200000", "tsz": "915757200000"}
            """));

        assertThat(result.get("i"), is(1));
        assertThat(result.get("l"), is(2L));
        assertThat(result.get("f"), is(0.12f));
        assertThat(result.get("d"), is(0.23d));
        assertThat(result.get("s"), is((short) 4));
        assertThat(result.get("b"), is((byte) 5));
        assertThat(result.get("ts"), is(915757200000L));
        assertThat(result.get("tsz"), is(915757200000L));
    }

    @Test
    public void test_string_encoded_boolean_will_be_parsed_by_data_type() {
        SourceParser sourceParser = new SourceParser();
        sourceParser.register(new ColumnIdent("_doc", List.of("b")), DataTypes.BOOLEAN);
        Map<String, Object> result = sourceParser.parse(new BytesArray(
            """
                {"b": "true"}
            """));

        assertThat(result.get("b"), is(true));
    }

    @Test
    public void test_uses_inner_type_info_to_parse_objects() throws Exception {
        SourceParser sourceParser = new SourceParser();
        BitStringType bitStringType = new BitStringType(4);
        ObjectType objectType = ObjectType.builder()
            .setInnerType("bs", bitStringType)
            .build();
        sourceParser.register(new ColumnIdent("_doc", List.of("o")), objectType);
        Map<String, Object> result = sourceParser.parse(new BytesArray(
            """
            {
                "o": {
                    "bs": "CQ=="
                }
            }
            """
        ));
        assertThat(result)
            .extracting("o")
            .extracting("bs")
            .isEqualTo(BitString.ofRawBits("1001"));
    }
}
