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

package io.crate.sql;


import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class LiteralsTest {

    @Test
    public void testEscape() throws Exception {
        assertThat(Literals.escapeStringLiteral(""), is(""));
        assertThat(Literals.escapeStringLiteral("foobar"), is("foobar"));
        assertThat(Literals.escapeStringLiteral("'"), is("''"));
        assertThat(Literals.escapeStringLiteral("''"), is("''''"));
        assertThat(Literals.escapeStringLiteral("'fooBar'"), is("''fooBar''"));
    }

    @Test
    public void testQuote() throws Exception {
        assertThat(Literals.quoteStringLiteral(""), is("''"));
        assertThat(Literals.quoteStringLiteral("foobar"), is("'foobar'"));
        assertThat(Literals.quoteStringLiteral("'"), is("''''"));
        assertThat(Literals.quoteStringLiteral("''"), is("''''''"));
        assertThat(Literals.quoteStringLiteral("'fooBar'"), is("'''fooBar'''"));
    }
}
