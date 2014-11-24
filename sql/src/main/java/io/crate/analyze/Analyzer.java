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

        AbstractStatementAnalyzer statementAnalyzer = dispatcher.process(statement, null);
        AnalyzedStatement analyzedStatement = statementAnalyzer.newAnalysis(parameterContext);
        statement.accept(statementAnalyzer, analyzedStatement);
        analyzedStatement.normalize();
        return new Analysis(analyzedStatement);
    }


    public static class AnalyzerDispatcher extends AstVisitor<AbstractStatementAnalyzer, Void> {

        private final SelectStatementAnalyzer selectStatementAnalyzer;
        private final InsertFromSubQueryAnalyzer insertFromSubQueryAnalyzer;
        private final UpdateStatementAnalyzer updateStatementAnalyzer;
        private final DeleteStatementAnalyzer deleteStatementAnalyzer;
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
                                  DeleteStatementAnalyzer deleteStatementAnalyzer,
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
            this.deleteStatementAnalyzer = deleteStatementAnalyzer;
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

        @Override
        protected AbstractStatementAnalyzer visitQuery(Query node, Void context) {
            return selectStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitDelete(Delete node, Void context) {
            return deleteStatementAnalyzer;
        }


        @Override
        public AbstractStatementAnalyzer visitInsertFromValues(InsertFromValues node, Void context) {
            return new InsertFromValuesAnalyzer(analysisMetaData);
        }

        @Override
        public AbstractStatementAnalyzer visitInsertFromSubquery(InsertFromSubquery node, Void context) {
            return insertFromSubQueryAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitUpdate(Update node, Void context) {
            return updateStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitCopyFromStatement(CopyFromStatement node, Void context) {
            return new CopyStatementAnalyzer(analysisMetaData);
        }

        @Override
        public AbstractStatementAnalyzer visitCopyTo(CopyTo node, Void context) {
            return new CopyStatementAnalyzer(analysisMetaData);
        }

        @Override
        public AbstractStatementAnalyzer visitDropTable(DropTable node, Void context) {
            return dropTableStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitCreateTable(CreateTable node, Void context) {
            return createTableStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitCreateAnalyzer(CreateAnalyzer node, Void context) {
            return createAnalyzerStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitCreateBlobTable(CreateBlobTable node, Void context) {
            return createBlobTableStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitDropBlobTable(DropBlobTable node, Void context) {
            return dropBlobTableStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitAlterBlobTable(AlterBlobTable node, Void context) {
            return alterBlobTableAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitRefreshStatement(RefreshStatement node, Void context) {
            return refreshTableAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitAlterTable(AlterTable node, Void context) {
            return alterTableAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitAlterTableAddColumnStatement(AlterTableAddColumn node, Void context) {
            return alterTableAddColumnAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitSetStatement(SetStatement node, Void context) {
            return setStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitResetStatement(ResetStatement node, Void context) {
            return setStatementAnalyzer;
        }

        @Override
        protected AbstractStatementAnalyzer visitNode(Node node, Void context) {
            throw new UnsupportedOperationException(String.format("cannot analyze statement: '%s'", node));
        }
    }
}
