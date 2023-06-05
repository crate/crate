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

import java.util.List;

import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.planner.node.management.ExplainPlan;
import io.crate.profile.ProfilingContext;
import io.crate.profile.Timer;
import io.crate.sql.SqlFormatter;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.CopyFrom;
import io.crate.sql.tree.Explain;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.Query;
import io.crate.sql.tree.Statement;

public class ExplainStatementAnalyzer {

    private final Analyzer analyzer;

    ExplainStatementAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    public ExplainAnalyzedStatement analyze(Explain node, Analysis analysis) {
        Statement statement = node.getStatement();
        statement.accept(CHECK_VISITOR, null);

        final AnalyzedStatement subStatement;
        ProfilingContext profilingContext;
        boolean showStats = false;
        if (node.mode() == Explain.Mode.ANALYZE) {
            profilingContext = new ProfilingContext(List.of());
            Timer timer = profilingContext.createAndStartTimer(ExplainPlan.Phase.Analyze.name());
            profilingContext.stopTimerAndStoreDuration(timer);
        } else if (node.mode() == Explain.Mode.STATS) {
            showStats = true;
            profilingContext = null;
        } else {
            profilingContext = null;
        }
        subStatement = analyzer.analyzedStatement(statement, analysis);
        String columnName = SqlFormatter.formatSql(node);
        return new ExplainAnalyzedStatement(columnName, subStatement, profilingContext, showStats);
    }

    private static final AstVisitor<Void, Void> CHECK_VISITOR = new AstVisitor<>() {

        @Override
        protected Void visitQuery(Query node, Void context) {
            return null;
        }

        @Override
        public Void visitCopyFrom(CopyFrom<?> node, Void context) {
            return null;
        }

        @Override
        protected Void visitNode(Node node, Void context) {
            throw new UnsupportedFeatureException("EXPLAIN is not supported for " + node);
        }
    };

}
