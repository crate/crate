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

import static io.crate.testing.Asserts.exactlyInstanceOf;
import static io.crate.testing.Asserts.isDocTable;
import static io.crate.testing.Asserts.isField;
import static io.crate.testing.Asserts.isReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedView;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class SelectFromViewAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("create table doc.t1 (name string, x int)")
            .addView(new RelationName("doc", "v1"), "select name, count(*) from doc.t1 group by name");
    }

    @Test
    public void testSelectFromViewIsResolvedToViewQueryDefinition() {
        QueriedSelectRelation query = e.analyze("select * from doc.v1");
        assertThat(query.outputs()).satisfiesExactly(isField("name"), isField("count(*)"));
        assertThat(query.groupBy()).isEmpty();
        assertThat(query.from()).satisfiesExactly(exactlyInstanceOf(AnalyzedView.class));
        QueriedSelectRelation queriedDocTable = (QueriedSelectRelation) ((AnalyzedView) query.from().get(0)).relation();
        assertThat(queriedDocTable.groupBy()).satisfiesExactly(isReference("name"));
        assertThat(queriedDocTable.from()).satisfiesExactly(isDocTable(new RelationName("doc", "t1")));
    }

    @Test
    public void test_fqn_with_catalog() {
        AnalyzedRelation relation = e.analyze("select * from crate.doc.v1");
        Assertions.assertThat(relation.outputs()).hasSize(2);

        relation = e.analyze("select crate.doc.v1.name from crate.doc.v1");
        Assertions.assertThat(relation.outputs()).hasSize(1);
        Assertions.assertThat(relation.outputs().get(0).toColumn().fqn()).isEqualTo("name");

        relation = e.analyze("select crate.doc.v1.name from v1");
        Assertions.assertThat(relation.outputs()).hasSize(1);
        Assertions.assertThat(relation.outputs().get(0).toColumn().fqn()).isEqualTo("name");

        relation = e.analyze("select v.name from crate.doc.v1 as v");
        Assertions.assertThat(relation.outputs()).hasSize(1);
        Assertions.assertThat(relation.outputs().get(0).toColumn().fqn()).isEqualTo("name");
    }

    @Test
    public void test_fqn_with_invalid_catalog() {
        assertThatThrownBy(
            () -> e.analyze("select * from \"invalidCatalog\".doc.v1"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unexpected catalog name: invalidCatalog. Only available catalog is crate");
        assertThatThrownBy(
            () -> e.analyze("select invalid.doc.t1.a from crate.doc.v1"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unexpected catalog name: invalid. Only available catalog is crate");
    }


    @Test
    public void test_anylze_with_changed_search_path() throws Exception {
        e.addTable("create table custom.t1 (name string, x int)");
        e.setSearchPath("custom");
        e.addView(new RelationName("doc", "v1"), "select name, count(*) from t1 group by name");
        e.setSearchPath("foobar");
        QueriedSelectRelation relation = e.analyze("select * from doc.v1");
        List<AnalyzedRelation> sources = relation.from();
        assertThat(sources).satisfiesExactly(
            from -> assertThat(from.relationName()).isEqualTo(new RelationName("doc", "v1")));
        AnalyzedView viewQuery = (AnalyzedView) sources.get(0);
        assertThat(((QueriedSelectRelation) viewQuery.relation()).from()).satisfiesExactly(
            from -> assertThat(from.relationName()).isEqualTo(new RelationName("custom", "t1")));
    }
}
