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

import java.util.Locale;
import java.util.Map;

import org.junit.Test;

import io.crate.common.collections.MapBuilder;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

public class ParseURIFunctionTest extends ScalarTestCase {

    @Test
    public void test_null_input() {
        assertEvaluateNull("parse_uri(null)");
    }

    @Test
    public void test_parse_uri() {
        String uri = "https://crate.io/index.html";
        Map<String, Object> value = MapBuilder.<String, Object>newMapBuilder()
            .put("scheme", "https")
            .put("userinfo", null)
            .put("hostname", "crate.io")
            .put("port", null)
            .put("path", "/index.html")
            .put("query", null)
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
            .build();
        assertNormalize(String.format(Locale.ENGLISH,"parse_uri('%s')", uri), isLiteral(value, expectedType));
    }

    @Test
    public void test_parse_uri_userinfo() {
        String uri = "https://user:pwd@crate.io/";
        Map<String, Object> value = MapBuilder.<String, Object>newMapBuilder()
            .put("scheme", "https")
            .put("userinfo", "user:pwd")
            .put("hostname", "crate.io")
            .put("port", null)
            .put("path", "/")
            .put("query", null)
            .put("fragment", null)
            .map();
        assertEvaluate(String.format(Locale.ENGLISH,"parse_uri('%s')", uri), value);
    }

    @Test
    public void test_parse_uri_query() {
        String uri = "https://crate.io/?foo=bar&foo=bar2&foo2";
        Map<String, Object> value = MapBuilder.<String, Object>newMapBuilder()
            .put("scheme", "https")
            .put("userinfo", null)
            .put("hostname", "crate.io")
            .put("port", null)
            .put("path", "/")
            .put("query", "foo=bar&foo=bar2&foo2")
            .put("fragment", null)
            .map();
        assertEvaluate(String.format(Locale.ENGLISH,"parse_uri('%s')", uri), value);
    }

    @Test
    public void test_parse_uri_complete_example() {
        String uri = "https://user:pw%26@testing.crate.io:4200/data/index.html?foo=bar&foo=&foo2=https%3A%2F%2Fcrate.io%2F%3Ffoo%3Dbar%26foo%3Dbar2%26foo2#ref";
        Map<String, Object> value = MapBuilder.<String, Object>newMapBuilder()
            .put("scheme", "https")
            .put("userinfo", "user:pw&")
            .put("hostname", "testing.crate.io")
            .put("port", 4200)
            .put("path", "/data/index.html")
            .put("query", "foo=bar&foo=&foo2=https://crate.io/?foo=bar&foo=bar2&foo2")
            .put("fragment", "ref")
            .map();
        assertEvaluate(String.format(Locale.ENGLISH,"parse_uri('%s')", uri), value);
    }
}
