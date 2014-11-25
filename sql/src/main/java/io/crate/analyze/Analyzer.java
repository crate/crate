/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.sql.tree.*;
import org.elasticsearch.common.inject.Inject;

public class Analyzer {

    private final AnalyzerDispatcher dispatcher;

    private final static Object[] EMPTY_ARGS = new Object[0];
    private final static Object[][] EMPTY_BULK_ARGS = new Object[0][];

    @Inject
    public Analyzer(AnalyzerDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public Analysis analyze(Statement statement) {
        return analyze(statement, EMPTY_ARGS, EMPTY_BULK_ARGS);
    }

    public Analysis analyze(Statement statement, Object[] parameters, Object[][] bulkParams) {
        ParameterContext parameterContext = new ParameterContext(parameters, bulkParams);
        AnalyzedStatement analyzedStatement = dispatcher.process(statement, parameterContext);
        return new Analysis(analyzedStatement);
    }


    public static class AnalyzerDispatcher extends AstVisitor<AnalyzedStatement, ParameterContext> {

        private final SelectStatementAnalyzer selectStatementAnalyzer;
        private final InsertFromSubQueryAnalyzer insertFromSubQueryAnalyzer;
        private final UpdateStatementAnalyzer updateStatementAnalyzer;
        private final DropTableStatementAnalyzer dropTableStatementAnalyzer;
        private final CreateTableStatementAnalyzer createTableStatementAnalyzer;
        private final CreateBlobTableStatementAnalyzer createBlobTableStatementAnalyzer;
        private final CreateAnalyzerStatementAnalyzer createAnalyzerStatementAnalyzer;
        private final DropBlobTableStatementAnalyzer dropBlobTableStatementAnalyzer;
        private final RefreshTableAnalyzer refreshTableAnalyzer;
        private final AlterTableAnalyzer alterTableAnalyzer;
        private final AlterBlobTableAnalyzer alterBlobTableAnalyzer;
        private final SetStatementAnalyzer setStatementAnalyzer;
        private final AlterTableAddColumnAnalyzer alterTableAddColumnAnalyzer;
        private AnalysisMetaData analysisMetaData;

        @Inject
        public AnalyzerDispatcher(AnalysisMetaData analysisMetaData,
                                  SelectStatementAnalyzer selectStatementAnalyzer,
                                  InsertFromSubQueryAnalyzer insertFromSubQueryAnalyzer,
                                  UpdateStatementAnalyzer updateStatementAnalyzer,
                                  DropTableStatementAnalyzer dropTableStatementAnalyzer,
                                  CreateTableStatementAnalyzer createTableStatementAnalyzer,
                                  CreateBlobTableStatementAnalyzer createBlobTableStatementAnalyzer,
                                  CreateAnalyzerStatementAnalyzer createAnalyzerStatementAnalyzer,
                                  DropBlobTableStatementAnalyzer dropBlobTableStatementAnalyzer,
                                  RefreshTableAnalyzer refreshTableAnalyzer,
                                  AlterTableAnalyzer alterTableAnalyzer,
                                  AlterBlobTableAnalyzer alterBlobTableAnalyzer,
                                  SetStatementAnalyzer setStatementAnalyzer,
                                  AlterTableAddColumnAnalyzer alterTableAddColumnAnalyzer) {
            this.analysisMetaData = analysisMetaData;
            this.selectStatementAnalyzer = selectStatementAnalyzer;
            this.insertFromSubQueryAnalyzer = insertFromSubQueryAnalyzer;
            this.updateStatementAnalyzer = updateStatementAnalyzer;
            this.dropTableStatementAnalyzer = dropTableStatementAnalyzer;
            this.createTableStatementAnalyzer = createTableStatementAnalyzer;
            this.createBlobTableStatementAnalyzer = createBlobTableStatementAnalyzer;
            this.createAnalyzerStatementAnalyzer = createAnalyzerStatementAnalyzer;
            this.dropBlobTableStatementAnalyzer = dropBlobTableStatementAnalyzer;
            this.refreshTableAnalyzer = refreshTableAnalyzer;
            this.alterTableAnalyzer = alterTableAnalyzer;
            this.alterBlobTableAnalyzer = alterBlobTableAnalyzer;
            this.setStatementAnalyzer = setStatementAnalyzer;
            this.alterTableAddColumnAnalyzer = alterTableAddColumnAnalyzer;
        }

        private AnalyzedStatement analyze(Node node, AbstractStatementAnalyzer statementAnalyzer, ParameterContext parameterContext) {
            AnalyzedStatement analyzedStatement = statementAnalyzer.newAnalysis(parameterContext);
            node.accept(statementAnalyzer, analyzedStatement);
            analyzedStatement.normalize();
            return analyzedStatement;
        }

        @Override
        protected AnalyzedStatement visitQuery(Query node, ParameterContext context) {
            return analyze(node, selectStatementAnalyzer, context);
        }

        @Override
        public AnalyzedStatement visitDelete(Delete node, ParameterContext context) {
            DeleteStatementAnalyzer deleteStatementAnalyzer = new DeleteStatementAnalyzer(analysisMetaData, context);
            return deleteStatementAnalyzer.process(node, null);
        }

        @Override
        public AnalyzedStatement visitInsertFromValues(InsertFromValues node, ParameterContext context) {
            return analyze(node, new InsertFromValuesAnalyzer(analysisMetaData), context);
        }

        @Override
        public AnalyzedStatement visitInsertFromSubquery(InsertFromSubquery node, ParameterContext context) {
            return analyze(node, insertFromSubQueryAnalyzer, context);
        }

        @Override
        public AnalyzedStatement visitUpdate(Update node, ParameterContext context) {
            return analyze(node, updateStatementAnalyzer, context);
        }

        @Override
        public AnalyzedStatement visitCopyFromStatement(CopyFromStatement node, ParameterContext context) {
            return analyze(node, new CopyStatementAnalyzer(analysisMetaData), context);
        }

        @Override
        public AnalyzedStatement visitCopyTo(CopyTo node, ParameterContext context) {
            return analyze(node, new CopyStatementAnalyzer(analysisMetaData), context);
        }

        @Override
        public AnalyzedStatement visitDropTable(DropTable node, ParameterContext context) {
            return analyze(node, dropTableStatementAnalyzer, context);
        }

        @Override
        public AnalyzedStatement visitCreateTable(CreateTable node, ParameterContext context) {
            return analyze(node, createTableStatementAnalyzer, context);
        }

        @Override
        public AnalyzedStatement visitCreateAnalyzer(CreateAnalyzer node, ParameterContext context) {
            return analyze(node, createAnalyzerStatementAnalyzer, context);
        }

        @Override
        public AnalyzedStatement visitCreateBlobTable(CreateBlobTable node, ParameterContext context) {
            return analyze(node, createBlobTableStatementAnalyzer, context);
        }

        @Override
        public AnalyzedStatement visitDropBlobTable(DropBlobTable node, ParameterContext context) {
            return analyze(node, dropBlobTableStatementAnalyzer, context);
        }

        @Override
        public AnalyzedStatement visitAlterBlobTable(AlterBlobTable node, ParameterContext context) {
            return analyze(node, alterBlobTableAnalyzer, context);
        }

        @Override
        public AnalyzedStatement visitRefreshStatement(RefreshStatement node, ParameterContext context) {
            return analyze(node, refreshTableAnalyzer, context);
        }

        @Override
        public AnalyzedStatement visitAlterTable(AlterTable node, ParameterContext context) {
            return analyze(node, alterTableAnalyzer, context);
        }

        @Override
        public AnalyzedStatement visitAlterTableAddColumnStatement(AlterTableAddColumn node, ParameterContext context) {
            return analyze(node, alterTableAddColumnAnalyzer, context);
        }

        @Override
        public AnalyzedStatement visitSetStatement(SetStatement node, ParameterContext context) {
            return analyze(node, setStatementAnalyzer, context);
        }

        @Override
        public AnalyzedStatement visitResetStatement(ResetStatement node, ParameterContext context) {
            return analyze(node, setStatementAnalyzer, context);
        }

        @Override
        protected AnalyzedStatement visitNode(Node node, ParameterContext context) {
            throw new UnsupportedOperationException(String.format("cannot analyze statement: '%s'", node));
        }
    }
}
