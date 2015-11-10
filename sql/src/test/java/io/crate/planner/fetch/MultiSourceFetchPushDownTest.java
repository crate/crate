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

import io.crate.analyze.BaseAnalyzerTest;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.sql.tree.QualifiedName;
import io.crate.testing.MockedClusterServiceModule;
import io.crate.testing.T3;
import org.elasticsearch.common.inject.Module;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.core.Is.is;

public class MultiSourceFetchPushDownTest extends BaseAnalyzerTest {

    private MultiSourceSelect mss;
    private MultiSourceFetchPushDown pd;

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                new MockedClusterServiceModule(),
                T3.META_DATA_MODULE,
                new ScalarFunctionModule()
        ));
        return modules;
    }

    private void pushDown(String stmt) {
        SelectAnalyzedStatement a = (SelectAnalyzedStatement) analyze(stmt);
        assert a.relation() instanceof MultiSourceSelect;
        mss = (MultiSourceSelect) a.relation();
        pd = MultiSourceFetchPushDown.pushDown(mss);
    }

    private QuerySpec srcSpec(String tableName){
        return mss.sources().get(QualifiedName.of("doc", tableName)).querySpec();
    }

    @Test
    public void testPushDown() throws Exception {
        pushDown("select a, b from t1, t2");
        assertThat(pd.remainingOutputs(), isSQL("FETCH(INPUT(0), t1.a), FETCH(INPUT(1), t2.b)"));
        assertThat(mss.querySpec(), isSQL("SELECT RELCOL(doc.t1, 0), RELCOL(doc.t2, 0)"));
        assertThat(srcSpec("t1"), isSQL("SELECT t1._docid"));
        assertThat(srcSpec("t2"), isSQL("SELECT t2._docid"));

        assertThat(pd.fetchSources().size(), is(2));

    }

    @Test
    public void testPushDownWithOrder() throws Exception {
        pushDown("select a, b from t1, t2 order by b");
        assertThat(pd.remainingOutputs(), isSQL("FETCH(INPUT(0), t1.a), INPUT(1)"));
        assertThat(mss.querySpec(), isSQL("SELECT RELCOL(doc.t1, 0), RELCOL(doc.t2, 0) ORDER BY RELCOL(doc.t2, 0)"));
        assertThat(srcSpec("t1"), isSQL("SELECT t1._docid"));
        assertThat(srcSpec("t2"), isSQL("SELECT doc.t2.b ORDER BY doc.t2.b"));
    }

    @Test
    public void testPushDownWithMultiRelationOrder() throws Exception {
        pushDown("select a, b from t1, t2 order by x - y");
        assertThat(srcSpec("t1"), isSQL("SELECT t1._docid, doc.t1.x"));
        assertThat(srcSpec("t2"), isSQL("SELECT t2._docid, doc.t2.y"));
        assertThat(mss.querySpec(), isSQL("SELECT RELCOL(doc.t1, 0), RELCOL(doc.t1, 1), RELCOL(doc.t2, 0), RELCOL(doc.t2, 1), subtract(RELCOL(doc.t1, 1), RELCOL(doc.t2, 1)) ORDER BY subtract(RELCOL(doc.t1, 1), RELCOL(doc.t2, 1))"));
        assertThat(pd.remainingOutputs(), isSQL("FETCH(INPUT(0), t1.a), FETCH(INPUT(2), t2.b)"));
        assertThat(mss.remainingOrderBy().get().orderBySymbols(), isSQL("subtract(RELCOL(doc.t1, 1), RELCOL(doc.t2, 1))"));
    }
}