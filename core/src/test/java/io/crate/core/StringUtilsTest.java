/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.core;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StringUtilsTest {
    @Test
    public void testDottedToSQLPath() {
        assertEquals("a['b']", StringUtils.dottedToSqlPath("a.b"));
        assertEquals("a", StringUtils.dottedToSqlPath("a"));
        assertEquals("a['']", StringUtils.dottedToSqlPath("a."));
        assertEquals("a['b']['c']", StringUtils.dottedToSqlPath("a.b.c"));
    }

    @Test
    public void testSQLToDottedPath() {
        assertEquals("a.b", StringUtils.sqlToDottedPath("a['b']"));
        assertEquals("a", StringUtils.sqlToDottedPath("a"));
        assertEquals("a.", StringUtils.sqlToDottedPath("a['']"));
        assertEquals("a.b.c", StringUtils.sqlToDottedPath("a['b']['c']"));
        assertEquals("[]", StringUtils.sqlToDottedPath("[]"));
        assertEquals("a[abc]", StringUtils.sqlToDottedPath("a[abc]"));
    }

    @Test
    public void testPathListContainsPrefix() throws Exception {
        assertTrue(StringUtils.pathListContainsPrefix(Arrays.asList("a", "b"), "b"));
        assertTrue(StringUtils.pathListContainsPrefix(Arrays.asList("a", "b.c"), "b"));
        assertFalse(StringUtils.pathListContainsPrefix(Arrays.asList("a", "bc"), "b"));
    }

    @Test
    public void testGetPathListByPrefix() throws Exception {
        assertEquals(StringUtils.getPathByPrefix(Arrays.asList("a", "b"), "b"), "b");
        assertEquals(StringUtils.getPathByPrefix(Arrays.asList("a", "b.c"), "b"), "b.c");
        assertEquals(StringUtils.getPathByPrefix(Arrays.asList("a", "bc"), "b"), null);
    }

    @Test
    public void testCommonAncestors() throws Exception {
        assertEquals(ImmutableSet.of("a"), StringUtils.commonAncestors(Arrays.asList("a", "a.b")));

        assertEquals(ImmutableSet.of("d", "a", "b"),
                StringUtils.commonAncestors(Arrays.asList("a.c", "b", "b.c.d", "a", "a.b", "d")));

        assertEquals(ImmutableSet.of("d", "a", "b.c"),
                StringUtils.commonAncestors(Arrays.asList("a.c", "b.c", "b.c.d", "a", "a.b", "d")));
    }
}
