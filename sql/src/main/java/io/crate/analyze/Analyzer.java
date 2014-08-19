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

import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.ReferenceResolver;
import io.crate.planner.symbol.*;
import io.crate.sql.tree.*;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Inject;

import java.util.Locale;

public class Analyzer {

    private final AnalyzerDispatcher dispatcher;

    private final static Object[] EMPTY_ARGS = new Object[0];
    private final static Object[][] EMPTY_BULK_ARGS = new Object[0][];

    @Inject
    public Analyzer(ReferenceInfos referenceInfos,
                    Functions functions,
                    ReferenceResolver referenceResolver,
                    FulltextAnalyzerResolver fulltextAnalyzerResolver) {

        this.dispatcher = new AnalyzerDispatcher(
                referenceInfos, functions, referenceResolver, fulltextAnalyzerResolver);
    }

    public Analysis analyze(Statement statement) {
        return analyze(statement, EMPTY_ARGS, EMPTY_BULK_ARGS);
    }

    public Analysis analyze(Statement statement, Object[] parameters, Object[][] bulkParams) {
        Context ctx = new Context(parameters, bulkParams);
        AbstractStatementAnalyzer statementAnalyzer = dispatcher.process(statement, ctx);
        assert ctx.analysis != null;

        statement.accept(statementAnalyzer, ctx.analysis);
        ctx.analysis.normalize();
        return ctx.analysis;
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

                    if (expectedType == DataTypes.NULL) {
                        bulkTypes[i] = guessedType;
                    } else if (!bulkTypes[i].equals(guessedType)) {
                        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                "argument %d of bulk arguments contains mixed data types", i + 1));
                    }
                }
            }
        }

        private static DataType guessTypeSafe(Object value) throws IllegalArgumentException {
            DataType guessedType = DataTypes.guessType(value);
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

        public Symbol getAsSymbol(int index) {
            try {
                if (hasBulkParams()) {
                    // already did a type guess so it is possible to create a literal directly
                    return io.crate.planner.symbol.Literal.newLiteral(
                            bulkTypes[index], bulkParameters[currentIdx][index]);
                }
                return new Parameter(parameters[index]);
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "Tried to resolve a parameter but the arguments provided with the " +
                                "SQLRequest don't contain a parameter at position %d", index), e);
            }
        }
    }

    static class Context {
        final ParameterContext parameterCtx;
        Analysis analysis;

        private Context(Object[] parameters, Object[][] bulkParams) {
            this.parameterCtx = new ParameterContext(parameters, bulkParams);
        }
    }

    private static class AnalyzerDispatcher extends AstVisitor<AbstractStatementAnalyzer, Context> {

        private final ReferenceInfos referenceInfos;
        private final Functions functions;
        private final ReferenceResolver referenceResolver;
        private final FulltextAnalyzerResolver fulltextAnalyzerResolver;

        private final AbstractStatementAnalyzer selectStatementAnalyzer = new SelectStatementAnalyzer();
        private final AbstractStatementAnalyzer insertFromValuesAnalyzer = new InsertFromValuesAnalyzer();
        private final AbstractStatementAnalyzer insertFromSubQueryAnalyzer = new InsertFromSubQueryAnalyzer();
        private final AbstractStatementAnalyzer updateStatementAnalyzer = new UpdateStatementAnalyzer();
        private final AbstractStatementAnalyzer deleteStatementAnalyzer = new DeleteStatementAnalyzer();
        private final AbstractStatementAnalyzer copyStatementAnalyzer = new CopyStatementAnalyzer();
        private final AbstractStatementAnalyzer dropTableStatementAnalyzer = new DropTableStatementAnalyzer();
        private final AbstractStatementAnalyzer createTableStatementAnalyzer = new CreateTableStatementAnalyzer();
        private final AbstractStatementAnalyzer createBlobTableStatementAnalyzer = new CreateBlobTableStatementAnalyzer();
        private final AbstractStatementAnalyzer createAnalyzerStatementAnalyzer = new CreateAnalyzerStatementAnalyzer();
        private final AbstractStatementAnalyzer dropBlobTableStatementAnalyzer = new DropBlobTableStatementAnalyzer();
        private final AbstractStatementAnalyzer refreshTableAnalyzer = new RefreshTableAnalyzer();
        private final AbstractStatementAnalyzer alterTableAnalyzer = new AlterTableAnalyzer();
        private final AbstractStatementAnalyzer alterBlobTableAnalyzer = new AlterBlobTableAnalyzer();
        private final AbstractStatementAnalyzer setStatementAnalyzer = new SetStatementAnalyzer();
        private final AbstractStatementAnalyzer alterTableAddColumnAnalyzer = new AlterTableAddColumnAnalyzer();

        public AnalyzerDispatcher(ReferenceInfos referenceInfos,
                                  Functions functions,
                                  ReferenceResolver referenceResolver,
                                  FulltextAnalyzerResolver fulltextAnalyzerResolver) {
            this.referenceInfos = referenceInfos;
            this.functions = functions;
            this.referenceResolver = referenceResolver;
            this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
        }

        @Override
        protected AbstractStatementAnalyzer visitQuery(Query node, Context context) {
            context.analysis = new SelectAnalysis(
                    referenceInfos, functions, context.parameterCtx, referenceResolver);
            return selectStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitDelete(Delete node, Context context) {
            context.analysis = new DeleteAnalysis(
                    referenceInfos, functions, context.parameterCtx, referenceResolver);
            return deleteStatementAnalyzer;
        }


        @Override
        public AbstractStatementAnalyzer visitInsertFromValues(InsertFromValues node, Context context) {
            context.analysis = new InsertFromValuesAnalysis(
                    referenceInfos, functions, context.parameterCtx, referenceResolver);
            return insertFromValuesAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitInsertFromSubquery(InsertFromSubquery node, Context context) {
            context.analysis = new InsertFromSubQueryAnalysis(
                    referenceInfos, functions, context.parameterCtx, referenceResolver);
            return insertFromSubQueryAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitUpdate(Update node, Context context) {
            context.analysis = new UpdateAnalysis(
                    referenceInfos, functions, context.parameterCtx, referenceResolver);
            return updateStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitCopyFromStatement(CopyFromStatement node, Context context) {
            context.analysis = new CopyAnalysis(
                    referenceInfos, functions, context.parameterCtx, referenceResolver);
            return copyStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitCopyTo(CopyTo node, Context context) {
            context.analysis = new CopyAnalysis(
                    referenceInfos, functions, context.parameterCtx, referenceResolver);
            return copyStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitDropTable(DropTable node, Context context) {
            context.analysis = new DropTableAnalysis(referenceInfos);
            return dropTableStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitCreateTable(CreateTable node, Context context) {
            context.analysis = new CreateTableAnalysis(referenceInfos,
                    fulltextAnalyzerResolver,
                    context.parameterCtx);
            return createTableStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitCreateAnalyzer(CreateAnalyzer node, Context context) {
            context.analysis = new CreateAnalyzerAnalysis(fulltextAnalyzerResolver, context.parameterCtx);
            return createAnalyzerStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitCreateBlobTable(CreateBlobTable node, Context context) {
            context.analysis = new CreateBlobTableAnalysis(context.parameterCtx);
            return createBlobTableStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitDropBlobTable(DropBlobTable node, Context context) {
            context.analysis = new DropBlobTableAnalysis(context.parameterCtx, referenceInfos);
            return dropBlobTableStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitAlterBlobTable(AlterBlobTable node, Context context) {
            context.analysis = new AlterBlobTableAnalysis(context.parameterCtx, referenceInfos);
            return alterBlobTableAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitRefreshStatement(RefreshStatement node, Context context) {
            context.analysis = new RefreshTableAnalysis(referenceInfos, context.parameterCtx);
            return refreshTableAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitAlterTable(AlterTable node, Context context) {
            context.analysis = new AlterTableAnalysis(context.parameterCtx, referenceInfos);
            return alterTableAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitAlterTableAddColumnStatement(AlterTableAddColumn node, Context context) {
            context.analysis = new AddColumnAnalysis(
                    referenceInfos, fulltextAnalyzerResolver, context.parameterCtx);
            return alterTableAddColumnAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitSetStatement(SetStatement node, Context context) {
            context.analysis = new SetAnalysis(context.parameterCtx);
            return setStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitResetStatement(ResetStatement node, Context context) {
            context.analysis = new SetAnalysis(context.parameterCtx);
            return setStatementAnalyzer;
        }

        @Override
        protected AbstractStatementAnalyzer visitNode(Node node, Context context) {
            throw new UnsupportedOperationException(String.format("cannot analyze statement: '%s'", node));
        }
    }
}
