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

package io.crate.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import org.junit.Test;

public final class StringUtilsTest {

    @Test
    public void test_split_str_by_dots() throws Exception {
        var parts = StringUtils.splitToList('.', "a.b.c");
        assertThat(parts, contains("a", "b", "c"));
    }

    @Test
    public void test_split_str_with_repeated_delim() throws Exception {
        var parts = StringUtils.splitToList('.', "a..c");
        assertThat(parts, contains("a", "", "c"));
    }

    @Test
    public void test_split_empty_str() throws Exception {
        var parts = StringUtils.splitToList('.', "");
        assertThat(parts, contains(""));
    }
}
