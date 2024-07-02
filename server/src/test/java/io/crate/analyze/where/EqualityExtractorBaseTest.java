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

package io.crate.analyze.where;

import java.util.List;
import java.util.Map;

import org.junit.Before;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;

/**
 * Base class to prepare the boilerplate context (test tables, transaction context, etc.) necessary to test the
 * {@link EqualityExtractor}, and provide some useful methods for the tests.
 */
public abstract class EqualityExtractorBaseTest extends CrateDummyClusterServiceUnitTest {

    protected final CoordinatorTxnCtx coordinatorTxnCtx = new CoordinatorTxnCtx(CoordinatorSessionSettings.systemDefaults());
    private SqlExpressions expressions;
    protected EvaluatingNormalizer normalizer;
    private EqualityExtractor ee;

    @Before
    public void prepare() throws Exception {
        Map<RelationName, AnalyzedRelation> sources = T3.sources(List.of(T3.T1), clusterService);

        DocTableRelation tr1 = (DocTableRelation) sources.get(T3.T1);
        expressions = new SqlExpressions(sources, tr1);
        normalizer = EvaluatingNormalizer.functionOnlyNormalizer(expressions.nodeCtx);
        ee = new EqualityExtractor(normalizer);
    }

    /**
     * Helper method, used to test {@link EqualityExtractor#extractMatches(List, Symbol, TransactionContext)}
     */
    protected List<List<Symbol>> analyzeExact(Symbol query, List<ColumnIdent> primaryKeys) {
        return ee.extractMatches(primaryKeys, query, coordinatorTxnCtx).matches();
    }

    /**
     * Helper method, used to test {@link EqualityExtractor#extractParentMatches(List, Symbol, TransactionContext)}
     */
    protected List<List<Symbol>> analyzeParent(Symbol query, List<ColumnIdent> primaryKeys) {
        return ee.extractParentMatches(primaryKeys, query, coordinatorTxnCtx).matches();
    }

    /**
     * Convert a query expression as text into a normalized query {@link Symbol}.
     * @param expression The query expression as text
     * @return the query expression as Symbol
     */
    protected Symbol query(String expression) {
        return expressions.normalize(expressions.asSymbol(expression));
    }
}
