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

package io.crate.core.collections;

import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class StringObjectMapsTest extends CrateUnitTest {

    @Test
    public void testAccessByPath() throws Exception {
        Map<String, Object> map = new HashMap<>();
        assertNull(StringObjectMaps.getByPath(map, ""));
        assertNull(StringObjectMaps.getByPath(map, "a.b.c"));

        map.put("a", "b");

        assertThat((String) StringObjectMaps.getByPath(map, "a"), is("b"));
        assertNull(StringObjectMaps.getByPath(map, "a.b"));

        Map<String, Object> nestedMap = new HashMap<String, Object>() {{
            put("b", 123);
        }};
        map.put("a", nestedMap);
        assertThat((Map<String, Object>) StringObjectMaps.getByPath(map, "a"), is(nestedMap));
        assertThat((Integer) StringObjectMaps.getByPath(map, "a.b"), is(123));
        assertNull(StringObjectMaps.getByPath(map, "a.b.c"));
        assertNull(StringObjectMaps.getByPath(map, "a.c"));
        assertNull(StringObjectMaps.getByPath(map, "b.c"));
    }

    @Test
    public void testMergeIntoSourceWithNullEntry() {
        HashMap<String, Object> m = new HashMap<>();
        m.put("o", null);

        expectedException.expectMessage("Object o is null, cannot write x = 10 into it");
        StringObjectMaps.mergeInto(m, "o", Collections.singletonList("x"), 10);
    }

    @Test
    public void testMergeNestedIntoWithNullEntry() {
        HashMap<String, Object> m = new HashMap<>();
        m.put("o", null);

        expectedException.expectMessage("Object o is null, cannot write x.y = 10 into it");
        StringObjectMaps.mergeInto(m, "o", Arrays.asList("x", "y"), 10);
    }
}
