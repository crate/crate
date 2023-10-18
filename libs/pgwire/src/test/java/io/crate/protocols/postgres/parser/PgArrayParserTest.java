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

package io.crate.protocols.postgres.parser;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

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

    @SuppressWarnings("unchecked")
    private static <T> List<T> parse(String array, Function<byte[], Object> convert) {
        return (List<T>) PgArrayParser.parse(array, convert);
    }

    @Test
    public void test_string_array() {
        assertThat(parse("{\"a,\", \"ab\"}", String::new)).isEqualTo(List.of("a,", "ab"));
    }

    @Test
    public void test_empty_string_array() {
        assertThat(parse("{}", String::new)).isEqualTo(List.of());
    }

    @Test
    public void test_string_array_with_null_items() {
        assertThat(parse("{\"a\", NULL, NULL}", String::new)).isEqualTo(Arrays.asList("a", null, null));
    }

    @Test
    public void test_quoted_string_can_contain_curly_brackets() {
        assertThat(parse("{\"}}}{{{\"}", String::new)).isEqualTo(List.of("}}}{{{"));
    }

    @Test
    public void test_string_array_with_digits_and_no_quotes() {
        assertThat(parse("{23ab-38cd,42xy}", String::new)).isEqualTo(Arrays.asList("23ab-38cd", "42xy"));
    }

    @Test
    public void test_dash_and_underline_are_allowed_in_unquoted_strings() {
        assertThat(parse("{catalog_name,end-exec}", String::new)).containsExactly("catalog_name", "end-exec");
    }

    @Test
    public void test_two_dimensional_string_array() {
        assertThat(parse("{{\"a\", \"b\"}, {\"c\", \"d\"}}", String::new)).isEqualTo(List.of(
            List.of("a", "b"),
            List.of("c", "d")));
    }

    @Test
    public void test_three_dimensional_string_array() {
        assertThat(parse("{{{\"1\",\"2\"},{\"3\",\"4\"}},{{\"5\",\"6\"},{\"7\"}}}", String::new)).isEqualTo(List.of(
            List.of(
                List.of("1", "2"),
                List.of("3", "4")),
            List.of(
                List.of("5", "6"),
                List.of("7"))));
    }

    @Test
    public void test_integer_array() {
        assertThat(parse("{1, 2}", toInteger)).isEqualTo(List.of(1, 2));
    }

    @Test
    public void test_integer_array_with_quoted_items() {
        assertThat(parse("{\"2\", \"1\"}", toInteger)).isEqualTo(List.of(2, 1));
    }

    @Test
    public void test_decimal_array() {
        assertThat(parse("{-1.1, 2.3}", toDouble)).isEqualTo(List.of(-1.1, 2.3));
    }

    @Test
    public void test_decimal_array_with_quoted_items() {
        assertThat(parse("{\"1.1\", \"-2.3\"}", toDouble)).isEqualTo(List.of(1.1, -2.3));
    }

    @Test
    public void test_bool_array() {
        assertThat(parse("{true, false}", toBoolean)).isEqualTo(List.of(true, false));
    }

    @Test
    public void test_bool_array_with_quoted_items() {
        assertThat(parse("{\"false\",\"true\"}", toBoolean)).isEqualTo(List.of(false, true));
    }

    @Test
    public void test_unquoted_string_can_contain_whitespace() {
        assertThat(parse("{foo bar}", String::new)).isEqualTo(List.of("foo bar"));
    }

    @Test
    public void test_unquoted_string_trailing_whitespace_is_removed() {
        assertThat(parse("{foo  }", String::new)).isEqualTo(List.of("foo"));
    }

    @Test
    public void test_unquoted_string_leading_whitespace_is_removed() {
        assertThat(parse("{  foo}", String::new)).isEqualTo(List.of("foo"));
    }

    @Test
    public void test_json_array() {
        assertThat(parse("{\"{\\\"x\\\": 10.1}\",\"{\\\"y\\\": 20.2}\"}", toMap)).isEqualTo(List.of(Map.of("x", 10.1),
                                                                                                    Map.of("y", 20.2)));
    }

    @Test
    public void test_two_dimensional_json_array() {
        assertThat(parse(
            "{" +
            "   {" +
            "       \"{\\\"x\\\": 10.1}\"" +
            "   }," +
            "   {" +
            "       \"{\\\"y\\\": 20.2}\", \"{\\\"z\\\": \\\"test\\\"}\"" +
            "   }" +
            "}", toMap)).isEqualTo(List.of(
            List.of(Map.of("x", 10.1)),
            List.of(Map.of("y", 20.2), Map.of("z", "test"))));
    }

    @Test
    public void test_three_dimensional_json_array() {
        assertThat(parse(
            "{" +
            "   {" +
            "       {" +
            "           \"{\\\"x\\\": 10.1}\"" +
            "       }," +
            "       {" +
            "           \"{\\\"y\\\": 20.2}\", \"{\\\"z\\\": \\\"test\\\"}\"" +
            "       }" +
            "   }" +
            "}", toMap)).isEqualTo(List.of(
            List.of(
                List.of(Map.of("x", 10.1)),
                List.of(Map.of("y", 20.2), Map.of("z", "test")))));
    }

    @Test
    public void test_point_format_string_array_parsed_as_string() {
        assertThat(parse("{\"(1.3, 2.1)\", \"(3.4, 5.1)\"}", String::new)).isEqualTo(List.of("(1.3, 2.1)",
                                                                                             "(3.4, 5.1)"));
    }

    @Test
    public void test_unquoted_item_can_contain_dots() {
        assertThat(parse("{foo.bar,two}", String::new)).isEqualTo(List.of(
            "foo.bar",
            "two"
        ));
    }
}
