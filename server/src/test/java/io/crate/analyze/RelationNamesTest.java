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

package io.crate.analyze;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import io.crate.types.DataTypes;

public class RelationNamesTest extends CrateDummyClusterServiceUnitTest {

    private SqlExpressions sqlExpressions;
    private Map<RelationName, AnalyzedRelation> sources;

    @Before
    public void prepare() throws Exception {
        sources = T3.sources(clusterService);
        sqlExpressions = new SqlExpressions(sources);
    }

    @Test
    public void test_finding_relations() {
        Symbol constantCondition = sqlExpressions.asSymbol("true");
        assertThat(RelationNames.getShallow(constantCondition)).isEqualTo(Set.of());

        constantCondition = sqlExpressions.asSymbol("t1.x = 4");
        assertThat(RelationNames.getShallow(constantCondition)).containsExactly(new RelationName("doc", "t1"));

        constantCondition = sqlExpressions.asSymbol("t1.x < 4");
        assertThat(RelationNames.getShallow(constantCondition)).containsExactly(new RelationName("doc", "t1"));

        constantCondition = sqlExpressions.asSymbol("t1.x = 4 AND true");
        assertThat(RelationNames.getShallow(constantCondition)).containsExactly(new RelationName("doc", "t1"));

        constantCondition = sqlExpressions.asSymbol("t1.x = 1 and t1.i = 10");
        assertThat(RelationNames.getShallow(constantCondition)).containsExactly(new RelationName("doc", "t1"));

        constantCondition = sqlExpressions.asSymbol("t1.x = 1 and t1.i = 10 and t2.y = 3");
        assertThat(RelationNames.getShallow(constantCondition)).containsExactly(new RelationName("doc", "t1"), new RelationName("doc", "t2"));

        constantCondition = sqlExpressions.asSymbol("t1.x = t2.y AND t2.y + t2.y = 4");
        assertThat(RelationNames.getShallow(constantCondition)).containsExactly(new RelationName("doc", "t1"), new RelationName("doc", "t2"));
    }

    @Test
    public void test_extract_table_ident_from_alias_symbol() {
        RelationName tableIdent = new RelationName("doc", "t");
        assertThat(
        RelationNames.getDeep(
            new AliasSymbol("r", new SimpleReference(
                new ReferenceIdent(tableIdent, "a"),
                RowGranularity.DOC,
                DataTypes.SHORT,
                0,
                null))
        )).isEqualTo(Set.of(tableIdent));
    }
}

