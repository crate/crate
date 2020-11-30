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

package io.crate.expression.reference.doc.lucene;


import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;

import org.elasticsearch.common.bytes.BytesArray;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.types.DataTypes;

public class SourceParserTest {

    @Test
    public void test_extract_single_value_from_json_with_multiple_columns() throws Exception {
        SourceParser sourceParser = new SourceParser();
        var column = new ColumnIdent("_doc", List.of("x"));
        sourceParser.register(column, DataTypes.INTEGER);
        sourceParser.parse(new BytesArray("""
            {"x": 10, "y": 20}
        """));

        assertThat(sourceParser.get(column), is(10));
        assertThat(sourceParser.get(new ColumnIdent("_doc", List.of("y"))), Matchers.nullValue());
    }

    @Test
    public void test_extract_object_value_from_json() throws Exception {
    }

    @Test
    public void test_extract_object_children_from_json() throws Exception {
        SourceParser sourceParser = new SourceParser();
        var column = new ColumnIdent("_doc", List.of("obj", "x"));
        sourceParser.register(column, DataTypes.INTEGER);
        sourceParser.parse(new BytesArray("""
            {"obj": {"x": 10}, "y": 20}
        """));
        assertThat(sourceParser.get(column), is(10));
        assertThat(sourceParser.get(new ColumnIdent("_doc", List.of("y"))), Matchers.nullValue());
    }

    @Test
    public void test_extract_array_value_from_json() throws Exception {
        SourceParser sourceParser = new SourceParser();
        var column = new ColumnIdent("_doc", List.of("x"));
        sourceParser.register(column, DataTypes.INTEGER_ARRAY);
        sourceParser.parse(new BytesArray("""
            {"x": [10, 11, 12], "y": 20}
        """));
        assertThat(sourceParser.get(column), is(List.of(10, 11, 12)));
        assertThat(sourceParser.get(new ColumnIdent("_doc", List.of("y"))), Matchers.nullValue());
    }

    @Test
    public void test_extract_value_from_object_array_json() throws Exception {
        SourceParser sourceParser = new SourceParser();
        var column = new ColumnIdent("_doc", List.of("obj_arr", "x"));
        sourceParser.register(column, DataTypes.INTEGER);
        sourceParser.parse(new BytesArray("""
            {"obj_arr": [{"x": 10}, {"x": 11}, {"x": 12}], "y": 20}
        """));
        assertThat(sourceParser.get(column), is(List.of(10, 11, 12)));
        assertThat(sourceParser.get(new ColumnIdent("_doc", List.of("y"))), Matchers.nullValue());
    }
}
