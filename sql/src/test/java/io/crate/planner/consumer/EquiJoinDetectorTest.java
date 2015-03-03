/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner.consumer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.metadata.*;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.operator.AndOperator;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Field;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.QualifiedName;
import io.crate.testing.MockedClusterServiceModule;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Map;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class EquiJoinDetectorTest {

    private EvaluatingNormalizer normalizer;

    private TableRelation t1 = new TableRelation(
            TestingTableInfo.builder(
                    new TableIdent(ReferenceInfos.DEFAULT_SCHEMA_NAME, "t1"),
                    RowGranularity.DOC,
                    new Routing())
                    .add("id", DataTypes.LONG, ImmutableList.<String>of())
                    .build());
    private TableRelation t2 = new TableRelation(
            TestingTableInfo.builder(
            new TableIdent(ReferenceInfos.DEFAULT_SCHEMA_NAME, "t2"),
    RowGranularity.DOC,
            new Routing())
            .add("id", DataTypes.LONG, ImmutableList.<String>of())
            .build());
    private Field t1Id = field(t1, "id", DataTypes.LONG);
    private Field t2Id = field(t2, "id", DataTypes.LONG);

    private final Map<QualifiedName, AnalyzedRelation> testMap = ImmutableMap.<QualifiedName, AnalyzedRelation>of(
            QualifiedName.of("t1"), t1,
            QualifiedName.of("t2"), t2
    );

    @Before
    public void prepare() {
        Injector injector = new ModulesBuilder()
                .add(new MockedClusterServiceModule())
                .add(new OperatorModule())
                .add(new ScalarFunctionModule())
                .add(new PredicateModule())
                .add(new MetaDataModule())
                .createInjector();
        normalizer = new EvaluatingNormalizer(
                injector.getInstance(Functions.class),
                RowGranularity.DOC,
                injector.getInstance(ReferenceResolver.class)
        );
    }

    /**
     * extract join condition from given whereClause that is not satisfiable by one of the sourceRelations
     */
    @Nullable
    private static  Symbol extractJoinCondition(WhereClause whereClause, Map<QualifiedName, AnalyzedRelation> sourceRelations) {
        if (!whereClause.hasQuery()) {
            return null;
        }
        Symbol query = whereClause.query();
        for (AnalyzedRelation analyzedRelation : sourceRelations.values()) {
            QuerySplitter.SplitQueries splitQueries = QuerySplitter.splitForRelation(analyzedRelation, query);
            query = splitQueries.remainingQuery();
        }
        if (query == null || query.symbolType().isLiteral()) {
            return null;
        }
        return query;
    }

    @Test
    public void testExtractJoinConditionInvalidWhereClauses() throws Exception {
        assertThat(extractJoinCondition(WhereClause.NO_MATCH, testMap), is(nullValue()));
        assertThat(extractJoinCondition(WhereClause.MATCH_ALL, testMap), is(nullValue()));
    }

    @Test
    public void testExtractNoJoinCondition() throws Exception {
        Symbol query = and(eq(Literal.newLiteral(1L), t1Id), eq(Literal.newLiteral(3L), t2Id));
        WhereClause whereClause = new WhereClause(query);
        assertThat(extractJoinCondition(whereClause, testMap), is(nullValue()));
    }

    @Test
    public void testExtractJoinCondition() throws Exception {
        Symbol query = and(eq(t1Id, t2Id), eq(Literal.newLiteral(3L), t1Id));
        WhereClause whereClause = new WhereClause(query);
        Symbol joinCondition = extractJoinCondition(whereClause, testMap);
        assertThat(joinCondition, is(notNullValue()));
        assertThat(joinCondition, isFunction(AndOperator.NAME));
        assertThat(((Function)joinCondition).arguments().get(0), isFunction(EqOperator.NAME));
        assertThat(((Function)joinCondition).arguments().get(1), isLiteral(true)); // replaced
    }

    private void assertHasEquiJoinCondition(Symbol query) {
        WhereClause whereClause = new WhereClause(query);
        Symbol joinCondition = normalizer.normalize(extractJoinCondition(whereClause, testMap));
        assertThat("not an equi join condition", EquiJoinDetector.isEquiJoinCondition(joinCondition), is(true));
    }

    private void assertHasNoEquiJoinCondition(Symbol query) {
        WhereClause whereClause = new WhereClause(query);
        Symbol joinCondition = normalizer.normalize(extractJoinCondition(whereClause, testMap));
        assertThat("equi join condition", EquiJoinDetector.isEquiJoinCondition(joinCondition), is(false));
    }

    @Test
    public void testEquiJoinConditions() throws Exception {
        assertHasEquiJoinCondition(and(eq(t1Id, t2Id), eq(Literal.newLiteral(3L), t1Id)));
        assertHasEquiJoinCondition(eq(t1Id, t2Id));
        assertHasEquiJoinCondition(and(eq(t1Id, t2Id), lt(Literal.newLiteral(3L), t1Id)));
        assertHasEquiJoinCondition(or(eq(t1Id, t2Id), Literal.newLiteral(false)));
    }

    @Test
    public void testNoEquiJoinConditions() throws Exception {
        assertHasNoEquiJoinCondition(eq(t1Id, Literal.newLiteral(2L)));
        assertHasNoEquiJoinCondition(or(eq(t1Id, t2Id), eq(Literal.newLiteral(3L), t1Id)));
        assertHasNoEquiJoinCondition(lt(t1Id, t2Id));
        assertHasNoEquiJoinCondition(and(eq(t1Id, t2Id), lt(t2Id, t1Id)));
        assertHasNoEquiJoinCondition(or(eq(t1Id, t2Id), eq(Literal.newLiteral(3L), t1Id)));
    }
}
