/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.protocols.postgres.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class PgArrayParserTest {

    private final Function<byte[], Object> toDouble = v -> Double.parseDouble(new String(v, UTF_8));
    private final Function<byte[], Object> toInteger = v -> Integer.parseInt(new String(v, UTF_8));
    private final Function<byte[], Object> toBoolean = v -> Boolean.parseBoolean(new String(v, UTF_8));
    private final Function<byte[], Object> toMap = v -> {
        try {
            return new ObjectMapper().readValue(v, Map.class);
        } catch (IOException e) {
            fail("cannot parse json string");
            return null;
        }
    };

    private static Object parse(String array, Function<byte[], Object> convert) {
        return PgArrayParser.parse(array.getBytes(UTF_8), convert);
    }

    @Test
    public void test_string_array() {
        assertThat(parse("{\"a,\", \"ab\"}", String::new), is(new Object[]{"a,", "ab"}));
    }

    @Test
    public void test_empty_string_array() {
        assertThat(parse("{}", String::new), is(new Object[]{}));
    }

    @Test
    public void test_string_array_with_null_items() {
        assertThat(parse("{\"a\", NULL, NULL}", String::new), is(new Object[]{"a", null, null}));
    }

    @Test
    public void test_two_dimensional_string_array() {
        assertThat(
            parse("{{\"a\", \"b\"}, {\"c\", \"d\"}}", String::new),
            is(new Object[]{
                new Object[]{"a", "b"},
                new Object[]{"c", "d"}
            }));
    }

    @Test
    public void test_three_dimensional_string_array() {
        assertThat(
            parse("{{{\"1\",\"2\"},{\"3\",\"4\"}},{{\"5\",\"6\"},{\"7\"}}}", String::new),
            is(new Object[]{
                new Object[]{
                    new Object[]{"1", "2"},
                    new Object[]{"3", "4"}},
                new Object[]{
                    new Object[]{"5", "6"},
                    new Object[]{"7"}}
            }));
    }

    @Test
    public void test_integer_array() {
        assertThat(parse("{1, 2}", toInteger), is(new Object[]{1, 2}));
    }

    @Test
    public void test_integer_array_with_quoted_items() {
        assertThat(parse("{\"2\", \"1\"}", toInteger), is(new Object[]{2, 1}));
    }

    @Test
    public void test_decimal_array() {
        assertThat(parse("{-1.1, 2.3}", toDouble), is(new Object[]{-1.1, 2.3}));
    }

    @Test
    public void test_decimal_array_with_quoted_items() {
        assertThat(parse("{\"1.1\", \"-2.3\"}", toDouble), is(new Object[]{1.1, -2.3}));
    }

    @Test
    public void test_bool_array() {
        assertThat(parse("{true, false}", toBoolean), is(new Object[]{true, false}));
    }

    @Test
    public void test_bool_array_with_quoted_items() {
        assertThat(parse("{\"false\", \"true\"}", toBoolean), is(new Object[]{false, true}));
    }

    @Test
    public void test_json_array() {
        assertThat(
            parse("{\"{\\\"x\\\": 10.1}\",\"{\\\"y\\\": 20.2}\"}", toMap),
            is(new Object[]{Map.of("x", 10.1), Map.of("y", 20.2)}));
    }

    @Test
    public void test_two_dimensional_json_array() {
        assertThat(
            parse(
                "{" +
                "   {" +
                "       \"{\\\"x\\\": 10.1}\"" +
                "   }," +
                "   {" +
                "       \"{\\\"y\\\": 20.2}\", \"{\\\"z\\\": \\\"test\\\"}\"" +
                "   }" +
                "}", toMap),
            is(new Object[]{
                new Object[]{Map.of("x", 10.1)},
                new Object[]{Map.of("y", 20.2), Map.of("z", "test")}}));
    }

    @Test
    public void test_three_dimensional_json_array() {
        assertThat(
            parse(
                "{" +
                "   {" +
                "       {" +
                "           \"{\\\"x\\\": 10.1}\"" +
                "       }," +
                "       {" +
                "           \"{\\\"y\\\": 20.2}\", \"{\\\"z\\\": \\\"test\\\"}\"" +
                "       }" +
                "   }" +
                "}", toMap),
            is(new Object[]{
                new Object[]{
                    new Object[]{Map.of("x", 10.1)},
                    new Object[]{Map.of("y", 20.2), Map.of("z", "test")}}}));
    }

    @Test
    public void test_point_format_string_array_parsed_as_string() {
        assertThat(
            parse("{\"(1.3, 2.1)\", \"(3.4, 5.1)\"}", String::new),
            is(new Object[]{"(1.3, 2.1)", "(3.4, 5.1)"}));
    }
}
