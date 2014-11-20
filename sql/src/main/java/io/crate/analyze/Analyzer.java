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
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Inject;

import java.util.Locale;

import static io.crate.planner.symbol.Literal.newLiteral;

public class Analyzer {

    private final AnalyzerDispatcher dispatcher;

    private final static Object[] EMPTY_ARGS = new Object[0];
    private final static Object[][] EMPTY_BULK_ARGS = new Object[0][];

    @Inject
    public Analyzer(AnalyzerDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public AnalyzedStatement analyze(Statement statement) {
        return analyze(statement, EMPTY_ARGS, EMPTY_BULK_ARGS);
    }

    public AnalyzedStatement analyze(Statement statement, Object[] parameters, Object[][] bulkParams) {
        ParameterContext parameterContext = new ParameterContext(parameters, bulkParams);

        AbstractStatementAnalyzer statementAnalyzer = dispatcher.process(statement, null);
        AnalyzedStatement analyzedStatement = statementAnalyzer.newAnalysis(parameterContext);
        statement.accept(statementAnalyzer, analyzedStatement);
        analyzedStatement.normalize();
        return analyzedStatement;
    }

    public static class ParameterContext {
        final Object[] parameters;
        final Object[][] bulkParameters;
        DataType[] bulkTypes;

        private int currentIdx = 0;

        public ParameterContext(Object[] parameters, Object[][] bulkParameters) {
            this.parameters = parameters;
            if (bulkParameters.length > 0) {
                validateBulkParams(bulkParameters);
            }
            this.bulkParameters = bulkParameters;
        }

        private void validateBulkParams(Object[][] bulkParams) {
            for (Object[] bulkParam : bulkParams) {
                if (bulkTypes == null) {
                    initializeBulkTypes(bulkParam);
                    continue;
                } else if (bulkParam.length != bulkTypes.length) {
                    throw new IllegalArgumentException("mixed number of arguments inside bulk arguments");
                }

                for (int i = 0; i < bulkParam.length; i++) {
                    Object o = bulkParam[i];
                    DataType expectedType = bulkTypes[i];
                    DataType guessedType = guessTypeSafe(o);

                    if (expectedType == DataTypes.UNDEFINED) {
                        bulkTypes[i] = guessedType;
                    } else if (o != null && !bulkTypes[i].equals(guessedType)) {
                        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                "argument %d of bulk arguments contains mixed data types", i + 1));
                    }
                }
            }
        }

        private static DataType guessTypeSafe(Object value) throws IllegalArgumentException {
            DataType guessedType = DataTypes.guessType(value, true);
            if (guessedType == null) {
                throw new IllegalArgumentException(String.format(
                        "Got an argument \"%s\" that couldn't be recognized", value));
            }
            return guessedType;
        }

        private void initializeBulkTypes(Object[] bulkParam) {
            bulkTypes = new DataType[bulkParam.length];
            for (int i = 0; i < bulkParam.length; i++) {
                bulkTypes[i] = guessTypeSafe(bulkParam[i]);
            }
        }

        public boolean hasBulkParams() {
            return bulkParameters.length > 0;
        }

        public void setBulkIdx(int i) {
            this.currentIdx = i;
        }

        public Object[] parameters() {
            if (hasBulkParams()) {
                return bulkParameters[currentIdx];
            }
            return parameters;
        }

        public io.crate.planner.symbol.Literal getAsSymbol(int index) {
            try {
                if (hasBulkParams()) {
                    // already did a type guess so it is possible to create a literal directly
                    return newLiteral(bulkTypes[index], bulkParameters[currentIdx][index]);
                }
                DataType type = guessTypeSafe(parameters[index]);
                // use type.value because some types need conversion (String to BytesRef, List to Array)
                return newLiteral(type, type.value(parameters[index]));
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "Tried to resolve a parameter but the arguments provided with the " +
                                "SQLRequest don't contain a parameter at position %d", index), e);
            }
        }
    }

    public static class AnalyzerDispatcher extends AstVisitor<AbstractStatementAnalyzer, Void> {

        private final SelectStatementAnalyzer selectStatementAnalyzer;
        private final InsertFromValuesAnalyzer insertFromValuesAnalyzer;
        private final InsertFromSubQueryAnalyzer insertFromSubQueryAnalyzer;
        private final UpdateStatementAnalyzer updateStatementAnalyzer;
        private final DeleteStatementAnalyzer deleteStatementAnalyzer;
        private final CopyStatementAnalyzer copyStatementAnalyzer;
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

        @Inject
        public AnalyzerDispatcher(SelectStatementAnalyzer selectStatementAnalyzer,
                                  InsertFromValuesAnalyzer insertFromValuesAnalyzer,
                                  InsertFromSubQueryAnalyzer insertFromSubQueryAnalyzer,
                                  UpdateStatementAnalyzer updateStatementAnalyzer,
                                  DeleteStatementAnalyzer deleteStatementAnalyzer,
                                  CopyStatementAnalyzer copyStatementAnalyzer,
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
            this.selectStatementAnalyzer = selectStatementAnalyzer;
            this.insertFromValuesAnalyzer = insertFromValuesAnalyzer;
            this.insertFromSubQueryAnalyzer = insertFromSubQueryAnalyzer;
            this.updateStatementAnalyzer = updateStatementAnalyzer;
            this.deleteStatementAnalyzer = deleteStatementAnalyzer;
            this.copyStatementAnalyzer = copyStatementAnalyzer;
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
            return insertFromValuesAnalyzer;
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
            return copyStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitCopyTo(CopyTo node, Void context) {
            return copyStatementAnalyzer;
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
