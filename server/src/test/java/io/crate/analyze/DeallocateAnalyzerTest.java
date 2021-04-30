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

package io.crate.analyze;

import io.crate.sql.tree.DeallocateStatement;
import io.crate.sql.tree.ParameterExpression;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class DeallocateAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService).build();
    }

    @Test
    public void testAnalyzeDeallocateAll() {
        AnalyzedDeallocate stmt = e.analyze("deallocate all");
        assertThat(stmt.preparedStmtName(), nullValue());
    }

    @Test
    public void testAnalyzeDeallocateWithLiteral() {
        AnalyzedDeallocate stmt = e.analyze("deallocate 'test_prep_stmt'");
        assertThat(stmt.preparedStmtName(), is("test_prep_stmt"));
    }

    @Test
    public void testAnalyzeDeallocateWithUnquotedLiteral() {
        AnalyzedDeallocate stmt = e.analyze("deallocate test_prep_stmt");
        assertThat(stmt.preparedStmtName(), is("test_prep_stmt"));
    }

    @Test
    public void testAnalyzeDeallocateWithLiteralLikeQualifiedName() {
        AnalyzedDeallocate stmt = e.analyze("deallocate test.prep.stmt");
        assertThat(stmt.preparedStmtName(), is("test.prep.stmt"));
    }

    @Test
    public void testAnalyzeDeallocateWithWrongExpression() {
        expectedException.expect(AssertionError.class);
        expectedException.expectMessage("Expression $1 not supported as preparedStmt expression for DEALLOCATE");
        DeallocateAnalyzer.analyze(new DeallocateStatement(new ParameterExpression(1)));
    }
}
