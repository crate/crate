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

package io.crate.expression.scalar.regex;

import io.crate.test.integration.CrateUnitTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.hamcrest.Matchers.contains;

public class RegexMatcherTest extends CrateUnitTest {

    @Test
    public void testMatch() throws Exception {
        String pattern = "hello";
        String text = "foobarbequebaz";
        RegexMatcher regexMatcher = new RegexMatcher(pattern);
        assertEquals(false, regexMatcher.match(text));
        assertThat(regexMatcher.groups(), Matchers.nullValue());

        pattern = "ba";
        regexMatcher = new RegexMatcher(pattern);
        assertEquals(true, regexMatcher.match(text));
        assertThat(regexMatcher.groups(), contains("ba"));

        pattern = "(ba)";
        regexMatcher = new RegexMatcher(pattern);
        assertEquals(true, regexMatcher.match(text));
        assertThat(regexMatcher.groups(), contains("ba"));

        pattern = ".*(ba).*";
        regexMatcher = new RegexMatcher(pattern);
        assertEquals(true, regexMatcher.match(text));
        assertThat(regexMatcher.groups(), contains("ba"));

        pattern = "((\\w+?)(ba))";
        regexMatcher = new RegexMatcher(pattern);
        assertEquals(true, regexMatcher.match(text));
        assertThat(regexMatcher.groups(), contains("fooba", "foo", "ba"));
    }

    @Test
    public void testReplaceNoMatch() throws Exception {
        String pattern = "crate";
        String text = "foobarbequebaz";
        String replacement = "crate";
        RegexMatcher regexMatcher = new RegexMatcher(pattern);
        assertEquals(text, regexMatcher.replace(text, replacement));
    }

    @Test
    public void testReplace() throws Exception {
        String pattern = "ba";
        String text = "foobarbequebaz";
        String replacement = "Crate";
        RegexMatcher regexMatcher = new RegexMatcher(pattern);
        assertEquals("fooCraterbequebaz", regexMatcher.replace(text, replacement));

        pattern = "(ba).*(ba)";
        replacement = "First$1Second$2";
        regexMatcher = new RegexMatcher(pattern);
        assertEquals("fooFirstbaSecondbaz", regexMatcher.replace(text, replacement));
    }

    @Test
    public void testReplaceGlobal() throws Exception {
        String pattern = "ba";
        String text = "foobarbequebaz";
        String replacement = "Crate";
        RegexMatcher regexMatcher = new RegexMatcher(pattern, "g");
        assertEquals("fooCraterbequeCratez", regexMatcher.replace(text, replacement));
    }

    @Test
    public void testMatchesNullGroup() throws Exception {
        String pattern = "\\w+( --?\\w+)*( \\w+)*";
        String text = "gcc -Wall --std=c99 -o source source.c";
        RegexMatcher regexMatcher = new RegexMatcher(pattern);
        assertEquals(true, regexMatcher.match(text));
        assertThat(regexMatcher.groups(), contains(" --std", null));
    }
}
