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

package io.crate.protocols.postgres.types;

import org.hamcrest.Matchers;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class PGArrayTest {

    private PGArray pgArray = PGArray.INT4_ARRAY;

    @Test
    public void testEncodeUTF8Text() throws Exception {
        // 1-dimension array
        byte[] bytes = pgArray.encodeAsUTF8Text(new Object[] { 10, 20 });
        String s = new String(bytes, StandardCharsets.UTF_8);
        assertThat(s, is("{\"10\",\"20\"}"));

        // 3-dimension array
        bytes = pgArray.encodeAsUTF8Text(new Object[][][] {{{1, 2}, {3, 4}}, {{5, 6}, {7}}});
        s = new String(bytes, StandardCharsets.UTF_8);
        assertThat(s, is("{{{\"1\",\"2\"},{\"3\",\"4\"}},{{\"5\",\"6\"},{\"7\"}}}"));
    }

    @Test
    public void testArrayWithNullValues() throws Exception {
        Object[] array = {10, null, 20};
        byte[] bytes = pgArray.encodeAsUTF8Text(array);
        String s = new String(bytes, StandardCharsets.UTF_8);
        assertThat(s, is("{\"10\",NULL,\"20\"}"));
        Object o = pgArray.decodeUTF8Text(bytes);
        assertThat(((Object[]) o), is(array));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testJsonArrayEncodeDecode() throws Exception {
        // 1-dimension array
        // Decode
        String s = "{\"{\"names\":[\"Arthur\",\"Trillian\"]}\",\"{\"names\":[\"Ford\",\"Slarti\"]}\"}";
        Object[] values = (Object[]) PGArray.JSON_ARRAY.decodeUTF8Text(s.getBytes(StandardCharsets.UTF_8));

        List<String> names = (List<String>) ((Map) values[0]).get("names");
        assertThat(names, Matchers.contains("Arthur", "Trillian"));

        names = (List<String>) ((Map) values[1]).get("names");
        assertThat(names, Matchers.contains("Ford", "Slarti"));

        // Encode
        byte[] bytes = PGArray.JSON_ARRAY.encodeAsUTF8Text(values);
        s = new String(bytes, StandardCharsets.UTF_8);
        assertThat(s, is(s));

        // 3-dimension array
        // Decode
        s = "{{{\"{\"names\":[\"A\",\"B\"]}\",\"{\"names\":[\"C\",\"D\"]}\"}," +
              "{\"{\"names\":[\"E\",\"F\"]}\",\"{\"names\":[\"G\",\"H\"]}\"}}," +
             "{{\"{\"names\":[\"I\",\"J\"]}\",\"{\"names\":[\"K\",\"L\"]}\"}," +
              "{\"{\"names\":[\"M\",\"N\"]}\",\"{\"names\":[\"O\",\"P\"]}\"}}}";
        values = (Object[]) PGArray.JSON_ARRAY.decodeUTF8Text(s.getBytes(StandardCharsets.UTF_8));

        names = (List<String>) ((Map) ((Object[])((Object[])values[0])[0])[0]).get("names");
        assertThat(names, Matchers.contains("A", "B"));
        names = (List<String>) ((Map) ((Object[])((Object[])values[0])[0])[1]).get("names");
        assertThat(names, Matchers.contains("C", "D"));

        names = (List<String>) ((Map) ((Object[])((Object[])values[0])[1])[0]).get("names");
        assertThat(names, Matchers.contains("E", "F"));
        names = (List<String>) ((Map) ((Object[])((Object[])values[0])[1])[1]).get("names");
        assertThat(names, Matchers.contains("G", "H"));

        names = (List<String>) ((Map) ((Object[])((Object[])values[1])[0])[0]).get("names");
        assertThat(names, Matchers.contains("I", "J"));
        names = (List<String>) ((Map) ((Object[])((Object[])values[1])[0])[1]).get("names");
        assertThat(names, Matchers.contains("K", "L"));

        names = (List<String>) ((Map) ((Object[])((Object[])values[1])[1])[0]).get("names");
        assertThat(names, Matchers.contains("M", "N"));
        names = (List<String>) ((Map) ((Object[])((Object[])values[1])[1])[1]).get("names");
        assertThat(names, Matchers.contains("O", "P"));

        // Encode
        bytes = PGArray.JSON_ARRAY.encodeAsUTF8Text(values);
        s = new String(bytes, StandardCharsets.UTF_8);
        assertThat(s, is(s));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDecodeEncodeEscapedJson() throws Exception {
        // Decode
        String s = "{\"{\\\"names\\\":[\\\"Arthur\\\",\\\"Trillian\\\"]}\",\"{\\\"names\\\":[\\\"Ford\\\",\\\"Slarti\\\"]}\"}";
        Object[] values = (Object[]) PGArray.JSON_ARRAY.decodeUTF8Text(s.getBytes(StandardCharsets.UTF_8));

        List<String> names = (List<String>) ((Map) values[0]).get("names");
        assertThat(names, Matchers.contains("Arthur", "Trillian"));

        names = (List<String>) ((Map) values[1]).get("names");
        assertThat(names, Matchers.contains("Ford", "Slarti"));

        // Encode
        byte[] bytes = PGArray.JSON_ARRAY.encodeAsUTF8Text(values);
        s = new String(bytes, StandardCharsets.UTF_8);
        assertThat(s, is("{\"{\"names\":[\"Arthur\",\"Trillian\"]}\",\"{\"names\":[\"Ford\",\"Slarti\"]}\"}"));
    }

    @Test
    public void testDecodeUTF8Text() throws Exception {
        // 1-dimension array
        Object o = pgArray.decodeUTF8Text("{\"10\",\"20\"}".getBytes(StandardCharsets.UTF_8));
        assertThat((Object[]) o, is(new Object[] {10, 20}));

        // 2-dimension array
        o = pgArray.decodeUTF8Text("{{\"1\",NULL,\"2\"},{NULL,\"3\",\"4\"}}".getBytes(StandardCharsets.UTF_8));
        assertThat(((Object[]) o), Is.<Object[]>is(new Object[][] {{1, null, 2}, {null, 3, 4}}));

        // 3-dimension array
        o = pgArray.decodeUTF8Text("{{{\"1\",NULL,\"2\"},{NULL,\"3\",\"4\"}},{{\"5\",NULL,\"6\"},{\"7\"}}".getBytes(StandardCharsets.UTF_8));
        assertThat(((Object[]) o), Is.<Object[]>is(new Object[][][] {{{1, null, 2}, {null, 3, 4}}, {{5, null, 6}, {7}}}));
    }
}
