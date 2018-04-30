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

import io.crate.analyze.relations.QueriedRelation;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isField;
import static org.hamcrest.Matchers.instanceOf;

public class ExplainAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    @Test
    public void testExplain() throws Exception {
        ExplainAnalyzedStatement stmt = e.analyze("explain select id from sys.cluster");
        assertNotNull(stmt.statement());
        assertThat(stmt.statement(), instanceOf(QueriedRelation.class));
    }

    @Test
    public void testExplainArrayComparison() throws Exception {
        ExplainAnalyzedStatement stmt = e.analyze("explain SELECT id from sys.cluster where id = any([1,2,3])");
        assertNotNull(stmt.statement());
        assertThat(stmt.statement(), instanceOf(QueriedRelation.class));
        assertThat(stmt.fields(), Matchers.contains(isField("EXPLAIN SELECT \"id\"\nFROM \"sys\".\"cluster\"\nWHERE \"id\" = ANY([1, 2, 3])\n")));
    }

    @Test
    public void testExplainCopyFrom() throws Exception {
        ExplainAnalyzedStatement stmt = e.analyze("explain copy users from '/tmp/*' WITH (shared=True)");
        assertThat(stmt.statement(), instanceOf(CopyFromAnalyzedStatement.class));
        assertThat(stmt.fields(), Matchers.contains(isField("EXPLAIN COPY \"users\" FROM '/tmp/*' WITH (\n   shared = true\n)")));
    }

    @Test
    public void testExplainRefreshUnsupported() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("EXPLAIN is not supported for RefreshStatement");
        e.analyze("explain refresh table parted");
    }

    @Test
    public void testExplainOptimizeUnsupported() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("EXPLAIN is not supported for OptimizeStatement");
        e.analyze("explain optimize table parted");
    }
}
