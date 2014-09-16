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

package io.crate.lucene;

import io.crate.analyze.WhereClause;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.OperatorModule;
import io.crate.planner.symbol.DataTypeSymbol;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataTypes;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.search.internal.SearchContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.createReference;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class LuceneQueryBuilderTest {

    private LuceneQueryBuilder builder;

    @Before
    public void setUp() throws Exception {
        Functions functions = new ModulesBuilder()
                .add(new OperatorModule()).createInjector().getInstance(Functions.class);
        builder = new LuceneQueryBuilder(functions,
                mock(SearchContext.class, Answers.RETURNS_MOCKS.get()),
                mock(IndexCache.class, Answers.RETURNS_MOCKS.get()));
    }

    @Test
    public void testWhereRefEqRef() throws Exception {
        Reference foo = createReference("foo", DataTypes.STRING);
        Query query = convert(eq(foo, foo));
        assertThat(query, instanceOf(FilteredQuery.class));
    }

    private Query convert(WhereClause eq) {
        return builder.convert(eq).query;
    }

    private WhereClause eq(DataTypeSymbol left, DataTypeSymbol right) {
        return new WhereClause(new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.asList(left.valueType(), right.valueType())), DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(left, right)
        ));
    }
}