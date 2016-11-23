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

package io.crate.planner.fetch;

import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class MultiSourceFetchPushDownTest extends CrateUnitTest {

    private MultiSourceSelect mss;
    private MultiSourceFetchPushDown pd;

    private SQLExecutor e = SQLExecutor.builder(new NoopClusterService()).enableDefaultTables().build();

    private void pushDown(String stmt) {
        SelectAnalyzedStatement a = e.analyze(stmt);
        assertThat(a.relation(), instanceOf(MultiSourceSelect.class));
        mss = (MultiSourceSelect) a.relation();
        pd = MultiSourceFetchPushDown.pushDown(mss);
    }

    private QuerySpec srcSpec(String tableName) {
        return mss.sources().get(QualifiedName.of("doc", tableName)).querySpec();
    }

    @Test
    public void testPushDown() throws Exception {
        pushDown("select a, b from t1, t2");
        assertThat(pd.remainingOutputs(), isSQL("FETCH(INPUT(0), doc.t1._doc['a']), FETCH(INPUT(1), doc.t2._doc['b'])"));
        assertThat(mss.querySpec(), isSQL("SELECT RELCOL(doc.t1, 0), RELCOL(doc.t2, 0)"));
        assertThat(srcSpec("t1"), isSQL("SELECT doc.t1._fetchid"));
        assertThat(srcSpec("t2"), isSQL("SELECT doc.t2._fetchid"));

        assertThat(pd.fetchSources().size(), is(2));

    }

    @Test
    public void testPushDownWithOrder() throws Exception {
        pushDown("select a, b from t1, t2 order by b");
        assertThat(pd.remainingOutputs(), isSQL("FETCH(INPUT(0), doc.t1._doc['a']), INPUT(1)"));
        assertThat(mss.querySpec(), isSQL("SELECT RELCOL(doc.t1, 0), RELCOL(doc.t2, 0) ORDER BY RELCOL(doc.t2, 0)"));
        assertThat(srcSpec("t1"), isSQL("SELECT doc.t1._fetchid"));
        assertThat(srcSpec("t2"), isSQL("SELECT doc.t2.b ORDER BY doc.t2.b"));
    }

}
