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
        byte[] bytes = pgArray.encodeAsUTF8Text(new Object[] { 10, 20 });
        String s = new String(bytes, StandardCharsets.UTF_8);

        assertThat(s, is("{\"10\",\"20\"}"));
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
        String s = "{\"{\"names\":[\"Arthur\",\"Trillian\"]}\",\"{\"names\":[\"Ford\",\"Slarti\"]}\"}";
        Object[] values = (Object[]) PGArray.JSON_ARRAY.decodeUTF8Text(s.getBytes(StandardCharsets.UTF_8));

        List<String> firstNames = (List<String>) ((Map) values[0]).get("names");
        assertThat(firstNames, Matchers.contains("Arthur", "Trillian"));

        List<String> secondNames = (List<String>) ((Map) values[1]).get("names");
        assertThat(secondNames, Matchers.contains("Ford", "Slarti"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDecodeEncodeEscapedJson() throws Exception {
        String s = "{\"{\\\"names\\\":[\\\"Arthur\\\",\\\"Trillian\\\"]}\",\"{\\\"names\\\":[\\\"Ford\\\",\\\"Slarti\\\"]}\"}";
        Object[] values = (Object[]) PGArray.JSON_ARRAY.decodeUTF8Text(s.getBytes(StandardCharsets.UTF_8));

        List<String> firstNames = (List<String>) ((Map) values[0]).get("names");
        assertThat(firstNames, Matchers.contains("Arthur", "Trillian"));

        List<String> secondNames = (List<String>) ((Map) values[1]).get("names");
        assertThat(secondNames, Matchers.contains("Ford", "Slarti"));
    }

    @Test
    public void testDecodeUTF8Text() throws Exception {
        Object o = pgArray.decodeUTF8Text("{\"10\",\"20\"}".getBytes(StandardCharsets.UTF_8));
        assertThat((Object[]) o, is(new Object[] { 10, 20}));
    }
}
