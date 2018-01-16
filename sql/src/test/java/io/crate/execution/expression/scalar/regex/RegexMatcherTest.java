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

package io.crate.execution.expression.scalar.regex;

import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import static org.hamcrest.Matchers.arrayContaining;

public class RegexMatcherTest extends CrateUnitTest {

    @Test
    public void testMatch() throws Exception {
        String pattern = "hello";
        BytesRef text = new BytesRef("foobarbequebaz");
        RegexMatcher regexMatcher = new RegexMatcher(pattern);
        assertEquals(false, regexMatcher.match(text));
        assertArrayEquals(null, regexMatcher.groups());

        pattern = "ba";
        regexMatcher = new RegexMatcher(pattern);
        assertEquals(true, regexMatcher.match(text));
        assertThat(regexMatcher.groups(), arrayContaining(new BytesRef("ba")));

        pattern = "(ba)";
        regexMatcher = new RegexMatcher(pattern);
        assertEquals(true, regexMatcher.match(text));
        assertThat(regexMatcher.groups(), arrayContaining(new BytesRef("ba")));

        pattern = ".*(ba).*";
        regexMatcher = new RegexMatcher(pattern);
        assertEquals(true, regexMatcher.match(text));
        assertThat(regexMatcher.groups(), arrayContaining(new BytesRef("ba")));

        pattern = "((\\w+?)(ba))";
        regexMatcher = new RegexMatcher(pattern);
        assertEquals(true, regexMatcher.match(text));
        assertThat(regexMatcher.groups(),
            arrayContaining(new BytesRef("fooba"), new BytesRef("foo"), new BytesRef("ba")));
    }

    @Test
    public void testReplaceNoMatch() throws Exception {
        String pattern = "crate";
        BytesRef text = new BytesRef("foobarbequebaz");
        String replacement = "crate";
        RegexMatcher regexMatcher = new RegexMatcher(pattern);
        assertEquals(text, regexMatcher.replace(text, replacement));
    }

    @Test
    public void testReplace() throws Exception {
        String pattern = "ba";
        BytesRef text = new BytesRef("foobarbequebaz");
        String replacement = "Crate";
        RegexMatcher regexMatcher = new RegexMatcher(pattern);
        assertEquals(new BytesRef("fooCraterbequebaz"), regexMatcher.replace(text, replacement));

        pattern = "(ba).*(ba)";
        replacement = "First$1Second$2";
        regexMatcher = new RegexMatcher(pattern);
        assertEquals(new BytesRef("fooFirstbaSecondbaz"), regexMatcher.replace(text, replacement));
    }

    @Test
    public void testReplaceGlobal() throws Exception {
        String pattern = "ba";
        BytesRef text = new BytesRef("foobarbequebaz");
        String replacement = "Crate";
        RegexMatcher regexMatcher = new RegexMatcher(pattern, new BytesRef("g"));
        assertEquals(new BytesRef("fooCraterbequeCratez"), regexMatcher.replace(text, replacement));
    }

    @Test
    public void testMatchesNullGroup() throws Exception {
        String pattern = "\\w+( --?\\w+)*( \\w+)*";
        BytesRef text = new BytesRef("gcc -Wall --std=c99 -o source source.c");
        RegexMatcher regexMatcher = new RegexMatcher(pattern);
        assertEquals(true, regexMatcher.match(text));
        assertThat(regexMatcher.groups(),
            arrayContaining(new BytesRef(" --std"), null));
    }
}
