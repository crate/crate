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

package io.crate.protocols.postgres.types;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.hamcrest.Matchers;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class PGArrayTest extends BasePGTypeTest<List<Object>> {

    private final PGArray pgArray = PGArray.INT4_ARRAY;

    public PGArrayTest() {
        super(PGArray.INT4_ARRAY);
    }

    @Test
    public void testEncodeUTF8Text() {
        // 1-dimension array
        byte[] bytes = pgArray.encodeAsUTF8Text(List.of(10, 20));
        String s = new String(bytes, StandardCharsets.UTF_8);
        assertThat(s).isEqualTo("{\"10\",\"20\"}");

        // 3-dimension array
        bytes = pgArray.encodeAsUTF8Text(List.of(List.of(List.of(1, 2), List.of(3, 4)), List.of(List.of(5, 6), List.of(7))));
        s = new String(bytes, StandardCharsets.UTF_8);
        assertThat(s).isEqualTo("{{{\"1\",\"2\"},{\"3\",\"4\"}},{{\"5\",\"6\"},{\"7\"}}}");
    }

    @Test
    public void testArrayWithNullValues() {
        List<Object> array = Arrays.asList(10, null, 20);
        byte[] bytes = pgArray.encodeAsUTF8Text(array);
        String s = new String(bytes, StandardCharsets.UTF_8);
        assertThat(s).isEqualTo("{\"10\",NULL,\"20\"}");
        Object o = pgArray.decodeUTF8Text(bytes);
        assertThat(o).isEqualTo(array);
    }

    public void test_json_array_encode_decode_round_trip() {
        List<Object> actual = List.of(
            Map.of("names", List.of("Arthur", "Trillian")),
            Map.of("names", List.of("Ford", "Slarti")));

        byte[] bytes = PGArray.JSON_ARRAY.encodeAsUTF8Text(actual);
        assertThat(
            new String(bytes, StandardCharsets.UTF_8)).isEqualTo("{" +
               "\"{\\\"names\\\":[\\\"Arthur\\\",\\\"Trillian\\\"]}\"," +
               "\"{\\\"names\\\":[\\\"Ford\\\",\\\"Slarti\\\"]}\"" +
               "}");

        assertThat(PGArray.JSON_ARRAY.decodeUTF8Text(bytes)).isEqualTo(actual);
    }

    @Test
    public void test_json_array_encode_decode_round_trip_with_escaped_quotes() {
        List<Object> actual = List.of(Map.of("values", List.of("foo \"")));
        byte[] bytes = PGArray.JSON_ARRAY.encodeAsUTF8Text(actual);
        assertThat(
            new String(bytes, StandardCharsets.UTF_8)).isEqualTo("{\"{\\\"values\\\":[\\\"foo \\\\\\\"\\\"]}\"}");
        assertThat(PGArray.JSON_ARRAY.decodeUTF8Text(bytes)).isEqualTo(actual);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDecodeEncodeEscapedJson() {
        // Decode
        String s = "{\"{\\\"names\\\":[\\\"Arthur\\\",\\\"Trillian\\\"]}\",\"{\\\"names\\\":[\\\"Ford\\\",\\\"Slarti\\\"]}\"}";
        List<Object> values = PGArray.JSON_ARRAY.decodeUTF8Text(s.getBytes(StandardCharsets.UTF_8));

        List<String> names = (List<String>) ((Map) values.get(0)).get("names");
        assertThat(names, Matchers.contains("Arthur", "Trillian"));

        names = (List<String>) ((Map) values.get(1)).get("names");
        assertThat(names, Matchers.contains("Ford", "Slarti"));

        // Encode
        byte[] bytes = PGArray.JSON_ARRAY.encodeAsUTF8Text(values);
        s = new String(bytes, StandardCharsets.UTF_8);
        assertThat(s).isEqualTo("{\"{\\\"names\\\":[\\\"Arthur\\\",\\\"Trillian\\\"]}\"," +
                         "\"{\\\"names\\\":[\\\"Ford\\\",\\\"Slarti\\\"]}\"}");
    }

    @Test
    public void test_can_decode_unquoted_strings_which_contain_numbers() throws Exception {
        String s = "{33a10b73-377d-4788-bbd3-9f0a0db4d595,2ea0876d-a290-438b-9816-cb4d63f48f77}";
        List<Object> values = PGArray.VARCHAR_ARRAY.decodeUTF8Text(s.getBytes(StandardCharsets.UTF_8));
        assertThat(values).containsExactly(
            "33a10b73-377d-4788-bbd3-9f0a0db4d595",
            "2ea0876d-a290-438b-9816-cb4d63f48f77"
        );
    }

    @Test
    public void test_can_decode_unquoted_strings_which_do_not_contain_numbers() throws Exception {
        String s = "{foo,bar}";
        List<Object> values = PGArray.VARCHAR_ARRAY.decodeUTF8Text(s.getBytes(StandardCharsets.UTF_8));
        assertThat(values).containsExactly(
            "foo",
            "bar"
        );
    }

    @Test
    public void test_can_decode_unquoted_words_with_multiple_spaces() throws Exception {
        String s = "{ \ta   b c  , \t d e   f  \t }";
        List<Object> values = PGArray.VARCHAR_ARRAY.decodeUTF8Text(s.getBytes(StandardCharsets.UTF_8));
        // leading/trailing whitespaces are ignored in the unquoted strings
        assertThat(values).containsExactly(
            "a   b c",
            "d e   f"
        );
    }

    @Test
    public void test_can_decode_quoted_words_with_multiple_spaces() throws Exception {
        String s = "{\" a b c \t \r\n \", \"  d \t\t  \\\" e \"}";
        List<Object> values = PGArray.VARCHAR_ARRAY.decodeUTF8Text(s.getBytes(StandardCharsets.UTF_8));
        // Quoting preserves leading/trailing whitespaces
        assertThat(values).containsExactly(
            " a b c \t \r\n ",
            "  d \t\t  \" e "
        );
    }

    @Test
    public void testDecodeUTF8Text() {
        // 1-dimension array
        Object o = pgArray.decodeUTF8Text("{\"10\",\"20\"}".getBytes(StandardCharsets.UTF_8));
        assertThat(o).isEqualTo(List.of(10, 20));

        // ensure unquoted integer values are decoded correctly (a bug prevented that once)
        o = pgArray.decodeUTF8Text("{10,2}".getBytes(StandardCharsets.UTF_8));
        assertThat(o).isEqualTo(List.of(10, 2));

        // ensure array with single value is decoded correctly (a bug prevented that once)
        o = pgArray.decodeUTF8Text("{10}".getBytes(StandardCharsets.UTF_8));
        assertThat(o).isEqualTo(List.of(10));

        // ensure that elements consisting of only a single character within an array can be decoded (a bug prevented that once)
        o = pgArray.decodeUTF8Text("{1}".getBytes(StandardCharsets.UTF_8));
        assertThat(o).isEqualTo(List.of(1));

        // 2-dimension array
        o = pgArray.decodeUTF8Text("{{\"1\",NULL,\"2\"},{NULL,\"3\",\"4\"}}".getBytes(StandardCharsets.UTF_8));
        assertThat(o).isEqualTo(List.of(Arrays.asList(1, null, 2), Arrays.asList(null, 3, 4)));

        // 3-dimension array
        o = pgArray.decodeUTF8Text("{{{\"1\",NULL,\"2\"},{NULL,\"3\",\"4\"}},{{\"5\",NULL,\"6\"},{\"7\"}}}".getBytes(StandardCharsets.UTF_8));
        assertThat(o, is(List.of(
            List.of(
                Arrays.asList(1, null, 2),
                Arrays.asList(null, 3, 4)),
            List.of(
                Arrays.asList(5, null, 6),
                List.of(7))))
        );
    }

    @Test
    public void testBinaryEncodingDecodingRoundtrip() {
        byte[] bytes = new byte[] {
            0, 0, 0, 44, // length as 4 byte int (not including the length itself)
            0, 0, 0, 1,  // dimensions as 4 byte int
            0, 0, 0, 1,  // possible nulls flag as 4 byte int
            0, 0, 0, 23, // oid of inner type (here is integer) as 4 byte int
            0, 0, 0, 3,  // dimension max elements as 4 byte int
            0, 0, 0, 3,  // dimension max elements as 4 byte int
            0, 0, 0, 4,  // length of inner type (here is integer) as 4 type int
            0, 0, 0, 1,  // value
            0, 0, 0, 4,  // length of inner type (here is integer) as 4 type int
            0, 0, 0, 2,  // value
            0, 0, 0, 4,  // length of inner type (here is integer) as 4 type int
            0, 0, 0, 3   // value
        };

        List<Object> source = List.of(1, 2, 3);
        assertBytesWritten(source, bytes, 48);

        ByteBuf buffer = Unpooled.wrappedBuffer(bytes);
        int length = buffer.readInt();
        Object targetArray = pgArray.readBinaryValue(buffer, length);
        buffer.release();
        assertThat(targetArray).isEqualTo(source);
    }

    @Test
    public void testBinaryEncodingDecodingRoundtrip_MultipleDimensionsWithNulls() {
        List<Object> sourceArray = List.of(
            Arrays.asList(1, 2, 3),
            Arrays.asList(4, 5, null),
            Arrays.asList(null, 6, 7, 8),
            Arrays.asList(null, null, 9, null)
        );

        ByteBuf buffer = Unpooled.buffer();
        pgArray.writeAsBinary(buffer, sourceArray);
        int length = buffer.readInt();
        Object targetArray = pgArray.readBinaryValue(buffer, length);
        buffer.release();

        // For multi-dimensions -1 is used both for "padding" until the max length of the dimension,
        // but also for null handling, therefore we cannot distinguish between the two.
        // That is the reason the output is different from the input
        List<Object> expectedArray = List.of(
            Arrays.asList(1, 2, 3, null),
            Arrays.asList(4, 5, null, null),
            Arrays.asList(null, 6, 7, 8),
            Arrays.asList(null, null, 9, null)
        );
        assertThat(targetArray).isEqualTo(expectedArray);
    }
}
