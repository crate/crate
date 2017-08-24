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

package io.crate.operation.operator;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.TransactionContext;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RegexpMatchCaseInsensitiveOperatorTest extends CrateUnitTest {
    private static Symbol normalizeSymbol(String source, String pattern) {
        RegexpMatchCaseInsensitiveOperator op = new RegexpMatchCaseInsensitiveOperator();
        Function function = new Function(
            op.info(),
            Arrays.<Symbol>asList(Literal.of(source), Literal.of(pattern))
        );
        return op.normalizeSymbol(function, new TransactionContext(SessionContext.create()));
    }

    private Boolean regexpNormalize(String source, String pattern) {
        return (Boolean) ((Literal) normalizeSymbol(source, pattern)).value();
    }

    @Test
    public void testNormalize() throws Exception {
        assertThat(regexpNormalize("", ""), is(true));
        assertThat(regexpNormalize("abc", "a.c"), is(true));
        assertThat(regexpNormalize("AbC", "a.c"), is(true));
        assertThat(regexpNormalize("abbbbc", "a(b{1,4})c"), is(true));
        assertThat(regexpNormalize("abc", "a~bc"), is(false));              // no PCRE syntax, should fail
        assertThat(regexpNormalize("100 €", "<10-101> €|$"), is(false));    // no PCRE syntax, should fail
    }

    @Test
    public void testNormalizeNull() throws Exception {
        assertThat(regexpNormalize(null, "foo"), is(nullValue()));
        assertThat(regexpNormalize("foo", null), is(nullValue()));
        assertThat(regexpNormalize(null, null), is(nullValue()));
    }

    // evaluate

    private Boolean regexpEvaluate(String source, String pattern) {
        RegexpMatchCaseInsensitiveOperator op = new RegexpMatchCaseInsensitiveOperator();
        return op.evaluate(Literal.of(source), Literal.of(pattern));
    }

    @Test
    public void testEvaluate() throws Exception {
        assertThat(regexpEvaluate("foo bar", "([A-Z][^ ]+ ?){2}"), is(true));   // case-insensitive matching should work
        assertThat(regexpEvaluate("Foo Bar", "([A-Z][^ ]+ ?){2}"), is(true));
        assertThat(regexpEvaluate("", ""), is(true));
        // java.util.regex does not understand proprietary syntax of `dk.brics.automaton` (no PCRE, should fail)
        assertThat(regexpEvaluate("1000 $", "(<1-9999>) $|€"), is(false));
        assertThat(regexpEvaluate("10000 $", "(<1-9999>) $|€"), is(false));
    }

    @Test
    public void testEvaluateNull() throws Exception {
        assertThat(regexpEvaluate(null, "foo"), is(nullValue()));
        assertThat(regexpEvaluate("foo", null), is(nullValue()));
        assertThat(regexpEvaluate(null, null), is(nullValue()));
    }
}
