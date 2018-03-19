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

package io.crate.analyze;

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
        DeallocateAnalyzedStatement stmt = e.analyze("deallocate all");
        assertThat(stmt.preparedStmtName(), nullValue());
    }

    @Test
    public void testAnalyzeDeallocateWithPrepStatementAsParameter() {
        String prepStatementName = "test_prep_stmt";
        DeallocateAnalyzedStatement stmt = e.analyze("deallocate $2", new Object[]{2, prepStatementName});
        assertThat(stmt.preparedStmtName(), is(prepStatementName));
        stmt = e.analyze("deallocate $1", new Object[]{prepStatementName});
        assertThat(stmt.preparedStmtName(), is(prepStatementName));
        stmt = e.analyze("deallocate ?", new Object[]{prepStatementName});
        assertThat(stmt.preparedStmtName(), is(prepStatementName));
    }

    @Test
    public void testAnalyzeDeallocateWithLiteral() {
        DeallocateAnalyzedStatement stmt = e.analyze("deallocate 'test_prep_stmt'");
        assertThat(stmt.preparedStmtName(), is("test_prep_stmt"));
    }

    @Test
    public void testAnalyzeDeallocateWithUnquotedLiteral() {
        DeallocateAnalyzedStatement stmt = e.analyze("deallocate test_prep_stmt");
        assertThat(stmt.preparedStmtName(), is("test_prep_stmt"));
    }
}
