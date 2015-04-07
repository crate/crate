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

    @Inject
    public Analyzer(AnalyzerDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public Analysis analyze(Statement statement, ParameterContext parameterContext) {
        Analysis analysis = new Analysis(parameterContext);
        AnalyzedStatement analyzedStatement = dispatcher.process(statement, analysis);
        assert analyzedStatement != null : "analyzed statement must not be null";
        analysis.analyzedStatement(analyzedStatement);
        return analysis;
    }

    public static class AnalyzerDispatcher extends AstVisitor<AnalyzedStatement, Analysis> {

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
        private final InsertFromValuesAnalyzer insertFromValuesAnalyzer;
        private final InsertFromSubQueryAnalyzer insertFromSubQueryAnalyzer;
        private final CopyStatementAnalyzer copyStatementAnalyzer;
        private final SelectStatementAnalyzer selectStatementAnalyzer;
        private final UpdateStatementAnalyzer updateStatementAnalyzer;
        private final DeleteStatementAnalyzer deleteStatementAnalyzer;


        @Inject
        public AnalyzerDispatcher(SelectStatementAnalyzer selectStatementAnalyzer,
                                  DropTableStatementAnalyzer dropTableStatementAnalyzer,
                                  CreateTableStatementAnalyzer createTableStatementAnalyzer,
                                  CreateBlobTableStatementAnalyzer createBlobTableStatementAnalyzer,
                                  CreateAnalyzerStatementAnalyzer createAnalyzerStatementAnalyzer,
                                  DropBlobTableStatementAnalyzer dropBlobTableStatementAnalyzer,
                                  RefreshTableAnalyzer refreshTableAnalyzer,
                                  AlterTableAnalyzer alterTableAnalyzer,
                                  AlterBlobTableAnalyzer alterBlobTableAnalyzer,
                                  SetStatementAnalyzer setStatementAnalyzer,
                                  AlterTableAddColumnAnalyzer alterTableAddColumnAnalyzer,
                                  InsertFromValuesAnalyzer insertFromValuesAnalyzer,
                                  InsertFromSubQueryAnalyzer insertFromSubQueryAnalyzer,
                                  CopyStatementAnalyzer copyStatementAnalyzer,
                                  UpdateStatementAnalyzer updateStatementAnalyzer,
                                  DeleteStatementAnalyzer deleteStatementAnalyzer) {
            this.selectStatementAnalyzer = selectStatementAnalyzer;
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
            this.insertFromValuesAnalyzer = insertFromValuesAnalyzer;
            this.insertFromSubQueryAnalyzer = insertFromSubQueryAnalyzer;
            this.copyStatementAnalyzer = copyStatementAnalyzer;
            this.updateStatementAnalyzer = updateStatementAnalyzer;
            this.deleteStatementAnalyzer = deleteStatementAnalyzer;
        }

        @Override
        protected AnalyzedStatement visitQuery(Query node, Analysis analysis) {
            return selectStatementAnalyzer.process(node, analysis);
        }

        @Override
        public AnalyzedStatement visitDelete(Delete node, Analysis context) {
            return deleteStatementAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitInsertFromValues(InsertFromValues node, Analysis context) {
            return insertFromValuesAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitInsertFromSubquery(InsertFromSubquery node, Analysis context) {
            return insertFromSubQueryAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitUpdate(Update node, Analysis context) {
            return updateStatementAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitCopyFromStatement(CopyFromStatement node, Analysis context) {
            return copyStatementAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitCopyTo(CopyTo node, Analysis context) {
            return copyStatementAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitDropTable(DropTable node, Analysis context) {
            return dropTableStatementAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitCreateTable(CreateTable node, Analysis analysis) {
            return createTableStatementAnalyzer.analyze(node, analysis);
        }

        @Override
        public AnalyzedStatement visitCreateAnalyzer(CreateAnalyzer node, Analysis context) {
            return createAnalyzerStatementAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitCreateBlobTable(CreateBlobTable node, Analysis context) {
            return createBlobTableStatementAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitDropBlobTable(DropBlobTable node, Analysis context) {
            return dropBlobTableStatementAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitAlterBlobTable(AlterBlobTable node, Analysis context) {
            return alterBlobTableAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitRefreshStatement(RefreshStatement node, Analysis context) {
            return refreshTableAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitAlterTable(AlterTable node, Analysis context) {
            return alterTableAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitAlterTableAddColumnStatement(AlterTableAddColumn node, Analysis context) {
            return alterTableAddColumnAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitSetStatement(SetStatement node, Analysis context) {
            return setStatementAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitResetStatement(ResetStatement node, Analysis context) {
            return setStatementAnalyzer.analyze(node, context);
        }

        @Override
        protected AnalyzedStatement visitNode(Node node, Analysis context) {
            throw new UnsupportedOperationException(String.format("cannot analyze statement: '%s'", node));
        }
    }
}
