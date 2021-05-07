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

package io.crate.analyze.where;

import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.eval.NullEliminator;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Function;

import static org.hamcrest.Matchers.is;

public class NullEliminatorTest extends CrateDummyClusterServiceUnitTest {

    private SqlExpressions sqlExpressions;

    @Before
    public void prepare() throws Exception {
        sqlExpressions = new SqlExpressions(T3.sources(clusterService));
    }

    private void assertReplaced(String expression, String expectedString) {
        assertReplaced(expression, expectedString,  s -> s);
    }

    private void assertReplacedAndNormalized(String expression, String expectedString) {
        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(sqlExpressions.nodeCtx);
        assertReplaced(expression, expectedString,  s -> normalizer.normalize(s, CoordinatorTxnCtx.systemTransactionContext()));
    }

    private void assertReplaced(String expression, String expectedString, Function<Symbol, Symbol> postProcessor) {
        Symbol query = sqlExpressions.asSymbol(expression);
        Symbol replacedQuery = NullEliminator.eliminateNullsIfPossible(query, postProcessor);
        assertThat(replacedQuery.toString(), is(expectedString));
    }

    @Test
    public void testNullsReplaced() throws Exception {
        sqlExpressions.context().allowEagerNormalize(false);
        assertReplaced("null and x = null", "(_cast(NULL, 'boolean') AND (x = _cast(NULL, 'integer')))");
        assertReplaced(
            "null or x = 1 or null",
            "((_cast(NULL, 'boolean') OR (x = 1)) OR _cast(NULL, 'boolean'))");
        assertReplaced(
            "not(null and x = 1)",
            "(NOT (_cast(NULL, 'boolean') AND (x = 1)))");
        assertReplaced(
            "not(null or not(null and x = 1))",
            "(NOT (_cast(NULL, 'boolean') OR (NOT (_cast(NULL, 'boolean') AND (x = 1)))))");
        assertReplaced(
            "not(null and x = 1) and not(null or x = 2)",
            "((NOT (_cast(NULL, 'boolean') AND (x = 1))) AND " +
            "(NOT (_cast(NULL, 'boolean') OR (x = 2))))");
        assertReplaced(
            "null or coalesce(null or x = 1, true)",
            "(_cast(NULL, 'boolean') OR coalesce((_cast(NULL, 'boolean') OR (x = 1)), true))");
    }

    @Test
    public void testNullsReplacedAndNormalized() {
        assertReplacedAndNormalized("null and x = 1", "false");
        assertReplacedAndNormalized("null or x > 1", "(x > 1)");
    }
}
