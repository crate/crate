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

import io.crate.analyze.relations.QueriedDocTable;
import io.crate.exceptions.AmbiguousColumnAliasException;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.testing.MockedClusterServiceModule;
import org.elasticsearch.common.inject.Module;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.T3.META_DATA_MODULE;
import static io.crate.testing.T3.T1_INFO;
import static io.crate.testing.TestingHelpers.isField;
import static org.hamcrest.Matchers.is;

public class SubSelectAnalyzerTest extends BaseAnalyzerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
            new MockedClusterServiceModule(),
            META_DATA_MODULE,
            new ScalarFunctionModule()
        ));
        return modules;
    }

    @Test
    public void testSimpleSubSelect() throws Exception {
        SelectAnalyzedStatement statement = analyze(
            "select aliased_sub.x / aliased_sub.i from (select x, i from t1) as aliased_sub");
        QueriedDocTable relation = (QueriedDocTable) statement.relation();
        assertThat(relation.fields().size(), is(1));
        assertThat(relation.fields().get(0), isField("(x / i)"));
        assertThat(relation.tableRelation().tableInfo(), is(T1_INFO));
    }

    @Test
    public void testSimpleSubSelectWithMixedCases() throws Exception {
        SelectAnalyzedStatement statement = analyze(
            "select aliased_sub.A from (select a from t1) as aliased_sub");
        QueriedDocTable relation = (QueriedDocTable) statement.relation();
        assertThat(relation.fields().size(), is(1));
        assertThat(relation.fields().get(0), isField("a"));
    }

    @Test
    public void testSubSelectWithoutAlias() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("subquery in FROM must have an alias");
        analyze("select id from (select a as id from t1)");
    }

    @Test
    public void testSubSelectWithNestedAlias() throws Exception {
        SelectAnalyzedStatement statement = analyze(
            "select tt.aa, (tt.xi + 1)" +
            " from (select (x + i) as xi, concat(a, a) as aa, i from t1) as tt");
        QueriedDocTable relation = (QueriedDocTable) statement.relation();
        assertThat(relation.fields().size(), is(2));
        assertThat(relation.fields().get(0), isField("aa"));
        assertThat(relation.fields().get(1), isField("(xi + 1)"));
        assertThat(relation.tableRelation().tableInfo(), is(T1_INFO));
    }

    @Test
    public void testNestedSubSelect() throws Exception {
        SelectAnalyzedStatement statement = analyze(
            "select aliased_sub.a from (select nested_sub.a from (select a from t1) as nested_sub) as aliased_sub");
        QueriedDocTable relation = (QueriedDocTable) statement.relation();
        assertThat(relation.fields().size(), is(1));
        assertThat(relation.fields().get(0), isField("a"));
    }

    @Test
    public void testSubSelectWithJoins() throws Exception {
        SelectAnalyzedStatement statement = analyze(
            "select aliased_sub.a, aliased_sub.b from (select t1.a, t2.b from t1, t2) as aliased_sub");
        MultiSourceSelect relation = (MultiSourceSelect) statement.relation();
        assertThat(relation.sources().size(), is(2));
        assertThat(relation.fields().size(), is(2));
        assertThat(relation.fields().get(0), isField("a"));
        assertThat(relation.fields().get(1), isField("b"));
    }

    @Test
    public void testSubSelectWithJoinsAmbiguousColumn() throws Exception {
        expectedException.expect(AmbiguousColumnAliasException.class);
        expectedException.expectMessage("Column alias \"i\" is ambiguous");
        analyze("select aliased_sub.i, aliased_sub.b from (select t1.i, t2.i, t2.b from t1, t2) as aliased_sub");
    }
}
