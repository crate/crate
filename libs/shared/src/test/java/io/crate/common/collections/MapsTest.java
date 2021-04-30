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

package io.crate.common.collections;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class MapsTest {

    @Test
    public void testAccessByPath() throws Exception {
        Map<String, Object> map = new HashMap<>();
        Assert.assertNull(Maps.getByPath(map, ""));
        Assert.assertNull(Maps.getByPath(map, "a.b.c"));

        map.put("a", "b");

        Assert.assertThat(Maps.getByPath(map, "a"), is("b"));
        Assert.assertNull(Maps.getByPath(map, "a.b"));

        Map<String, Object> nestedMap = new HashMap<String, Object>() {{
            put("b", 123);
        }};
        map.put("a", nestedMap);
        Assert.assertThat(Maps.getByPath(map, "a"), is(nestedMap));
        Assert.assertThat(Maps.getByPath(map, "a.b"), is(123));
        Assert.assertNull(Maps.getByPath(map, "a.b.c"));
        Assert.assertNull(Maps.getByPath(map, "a.c"));
        Assert.assertNull(Maps.getByPath(map, "b.c"));
    }

    @Test
    public void testMergeIntoSourceWithNullEntry() {
        HashMap<String, Object> m = new HashMap<>();
        m.put("o", null);

        Maps.mergeInto(m, "o", Collections.singletonList("x"), 10);
        Assert.assertThat(m, is(Map.of("o", Map.of("x", 10))));
    }

    @Test
    public void testMergeNestedIntoWithNullEntry() {
        HashMap<String, Object> m = new HashMap<>();
        m.put("o", null);

        Maps.mergeInto(m, "o", Arrays.asList("x", "y"), 10);
        Assert.assertThat(m, is(Map.of("o", Map.of("x", Map.of("y", 10)))));
    }
}
