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

package io.crate.expression.reference;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;

public class MapLookupByPathExpressionTest {

    @Test
    public void testInvalidPathResultsInNull() throws Exception {
        HashMap<String, Object> m = new HashMap<>();
        m.put("correct", 10);
        MapLookupByPathExpression<Map<String, Object>> expr =
            new MapLookupByPathExpression<>(Function.identity(), Collections.singletonList("incorrect"), Function.identity());

        expr.setNextRow(m);
        assertThat(expr.value(), Matchers.nullValue());
    }

    @Test
    public void testCorrectPathReturnsValue() throws Exception {
        HashMap<String, Object> m = new HashMap<>();
        m.put("correct", 10);
        MapLookupByPathExpression<Map<String, Object>> expr =
            new MapLookupByPathExpression<>(Function.identity(), Collections.singletonList("correct"), Function.identity());

        expr.setNextRow(m);
        assertThat(expr.value(), Matchers.is(10));
    }
}
