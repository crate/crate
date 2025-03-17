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

package io.crate.expression.scalar;

import static io.crate.testing.Asserts.isLiteral;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.Test;

import io.crate.common.collections.MapBuilder;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

public class ParseURLFunctionTest extends ScalarTestCase {

    @Test
    public void test_null_input() {
        assertEvaluateNull("parse_url(null)");
    }

    @Test
    public void test_parse_url() {
        String uri = "https://crate.io:8080/index.html";
        Map<String, Object> value = MapBuilder.<String, Object>newMapBuilder()
            .put("scheme", "https")
            .put("userinfo", null)
            .put("hostname", "crate.io")
            .put("port", 8080)
            .put("path", "/index.html")
            .put("query", null)
            .put("parameters", null)
            .put("fragment", null)
            .map();
        ObjectType expectedType = ObjectType.of(ColumnPolicy.DYNAMIC)
            .setInnerType("scheme", DataTypes.STRING)
            .setInnerType("userinfo", DataTypes.STRING)
            .setInnerType("hostname", DataTypes.STRING)
            .setInnerType("port", DataTypes.INTEGER)
            .setInnerType("path", DataTypes.STRING)
            .setInnerType("query", DataTypes.STRING)
            .setInnerType("fragment", DataTypes.STRING)
            .setInnerType("parameters", ObjectType.of(ColumnPolicy.DYNAMIC).build())
            .build();
        assertNormalize(String.format(Locale.ENGLISH,"parse_url('%s')", uri), isLiteral(value, expectedType));
    }

    @Test
    public void test_parse_url_userinfo() {
        String uri = "https://user:pwd@crate.io:443/";
        Map<String, Object> value = MapBuilder.<String, Object>newMapBuilder()
            .put("scheme", "https")
            .put("userinfo", "user:pwd")
            .put("hostname", "crate.io")
            .put("port", 443)
            .put("path", "/")
            .put("query", null)
            .put("parameters", null)
            .put("fragment", null)
            .map();
        assertEvaluate(String.format(Locale.ENGLISH,"parse_url('%s')", uri), value);
    }

    @Test
    public void test_parse_url_query() {
        String uri = "https://crate.io/?foo=bar&foo=bar2&foo2=bar&foo2";
        Map<String, Object> value = MapBuilder.<String, Object>newMapBuilder()
            .put("scheme", "https")
            .put("userinfo",null)
            .put("hostname", "crate.io")
            .put("port", null)
            .put("path", "/")
            .put("query", "foo=bar&foo=bar2&foo2=bar&foo2")
            .put("parameters", MapBuilder.<String,Object>newMapBuilder()
                                .put("foo",List.of("bar","bar2"))
                                .put("foo2",Stream.of("bar",null).toList())
                                .map())
            .put("fragment", null)
            .map();
        assertEvaluate(String.format(Locale.ENGLISH,"parse_url('%s')", uri), value);
    }

    @Test
    public void test_parse_url_unsafe_character() {
        String uri = "https://crate.io/sub%20space/hello.gif";
        Map<String, Object> value = MapBuilder.<String, Object>newMapBuilder()
            .put("scheme", "https")
            .put("userinfo",null)
            .put("hostname", "crate.io")
            .put("port", null)
            .put("path", "/sub space/hello.gif")
            .put("query", null)
            .put("parameters", null)
            .put("fragment", null)
            .map();
        assertEvaluate(String.format(Locale.ENGLISH,"parse_url('%s')", uri), value);
    }

    @Test
    public void test_parse_url_complete_example() {
        String uri = "https://user:pw%26@testing.crate.io:4200/sub+space/sub%20space2/index.html?foo=bar&foo=&foo2=https%3A%2F%2Fcrate.io%2F%3Ffoo%3Dbar%26foo%3Dbar2%26foo2#ref";
        Map<String, Object> value = MapBuilder.<String, Object>newMapBuilder()
            .put("scheme", "https")
            .put("userinfo", "user:pw&")
            .put("hostname", "testing.crate.io")
            .put("port", 4200)
            .put("path", "/sub space/sub space2/index.html")
            .put("query","foo=bar&foo=&foo2=https://crate.io/?foo=bar&foo=bar2&foo2")
            .put("parameters", MapBuilder.<String,Object>newMapBuilder()
                                .put("foo",Stream.of("bar",null).toList())
                                .put("foo2",List.of("https://crate.io/?foo=bar&foo=bar2&foo2"))
                                .map())
            .put("fragment", "ref")
            .map();
        assertEvaluate(String.format(Locale.ENGLISH,"parse_url('%s')", uri), value);
    }

}
