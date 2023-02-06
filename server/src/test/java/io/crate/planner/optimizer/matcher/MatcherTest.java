/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.optimizer.matcher;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import java.util.List;
import org.junit.Test;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.LogicalPlan;

public class MatcherTest {

    @Test
    public void test_type_of() {
        assertMatch(typeOf(Integer.class), 42);
        assertMatch(typeOf(Number.class), 42);
        assertNoMatch(typeOf(Integer.class), "foo");
        assertNoMatch(typeOf(Integer.class), null);
    }

    @Test
    public void test_with_property_matching() {
        assertMatch(typeOf(String.class).with(s -> s.length() == 3), "foo");
        var pattern = typeOf(String.class)
            .with(s -> s.startsWith("f"))
            .with((CharSequence s) -> s.length() < 5);
        assertMatch(pattern, "foo");
    }

    @Test
    public void test_with_source_matching() {
        Collect source = new Collect(mock(AbstractTableRelation.class), List.of(), WhereClause.MATCH_ALL, 1, 1);
        Filter filter = new Filter(source, mock(Symbol.class));
        var pattern = typeOf(Filter.class).with(source(), typeOf(Collect.class));
        assertMatch(pattern, filter);
    }

    @Test
    public void test_capture() {
        Filter source = new Filter(mock(LogicalPlan.class), mock(Symbol.class));
        Filter filter = new Filter(source, mock(Symbol.class));
        Capture<Filter> capture = new Capture<>();
        var pattern = typeOf(Filter.class).with(source(), typeOf(Filter.class)).capturedAs(capture);
        Match<Filter> match = pattern.match(filter, Captures.empty());
        assertMatch(pattern, filter);
        assertEquals(match.captures().get(capture), filter);
    }

    private <T> Match<T> assertMatch(Pattern<T> pattern, T expectedMatch) {
        Match<T> match = new DefaultMatcher().match(pattern, expectedMatch, Captures.empty());
        assertEquals(expectedMatch, match.value());
        return match;
    }

    private <T> void assertNoMatch(Pattern<T> pattern, Object expectedNoMatch) {
        Match<T> match = new DefaultMatcher().match(pattern, expectedNoMatch, Captures.empty());
        assertEquals(Match.empty(), match);
    }
}
