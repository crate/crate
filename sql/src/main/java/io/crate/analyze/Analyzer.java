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

import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.ReferenceResolver;
import io.crate.sql.tree.*;
import io.crate.metadata.FulltextAnalyzerResolver;
import org.elasticsearch.common.inject.Inject;

public class Analyzer {

    private final AnalyzerDispatcher dispatcher;

    @Inject
    public Analyzer(ReferenceInfos referenceInfos,
                    Functions functions,
                    ReferenceResolver referenceResolver,
                    FulltextAnalyzerResolver fulltextAnalyzerResolver) {

        this.dispatcher = new AnalyzerDispatcher(
                referenceInfos, functions, referenceResolver, fulltextAnalyzerResolver);
    }

    public Analysis analyze(Statement statement) {
        return analyze(statement, new Object[0]);
    }

    public Analysis analyze(Statement statement, Object[] parameters) {
        Context ctx = new Context(parameters);
        AbstractStatementAnalyzer statementAnalyzer = dispatcher.process(statement, ctx);
        assert ctx.analysis != null;

        statement.accept(statementAnalyzer, ctx.analysis);
        ctx.analysis.normalize();
        return ctx.analysis;
    }

    private static class Context {
        Object[] parameters;
        Analysis analysis;

        private Context(Object[] parameters) {
            this.parameters = parameters;
        }
    }

    private static class AnalyzerDispatcher extends AstVisitor<AbstractStatementAnalyzer, Context> {

        private final ReferenceInfos referenceInfos;
        private final Functions functions;
        private final ReferenceResolver referenceResolver;
        private final FulltextAnalyzerResolver fulltextAnalyzerResolver;

        private final AbstractStatementAnalyzer selectStatementAnalyzer = new SelectStatementAnalyzer();
        private final AbstractStatementAnalyzer insertStatementAnalyzer = new InsertStatementAnalyzer();
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
                    referenceInfos, functions, context.parameters, referenceResolver);
            return selectStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitDelete(Delete node, Context context) {
            context.analysis = new DeleteAnalysis(
                    referenceInfos, functions, context.parameters, referenceResolver);
            return deleteStatementAnalyzer;
        }


        @Override
        public AbstractStatementAnalyzer visitInsertFromValues(InsertFromValues node, Context context) {
            context.analysis = new InsertAnalysis(
                    referenceInfos, functions, context.parameters, referenceResolver);
            return insertStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitUpdate(Update node, Context context) {
            context.analysis = new UpdateAnalysis(
                    referenceInfos, functions, context.parameters, referenceResolver);
            return updateStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitCopyFromStatement(CopyFromStatement node, Context context) {
            context.analysis = new CopyAnalysis(
                    referenceInfos, functions, context.parameters, referenceResolver);
            return copyStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitCopyTo(CopyTo node, Context context) {
            context.analysis = new CopyAnalysis(
                    referenceInfos, functions, context.parameters, referenceResolver);
            return copyStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitDropTable(DropTable node, Context context) {
            context.analysis = new DropTableAnalysis(referenceInfos);
            return dropTableStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitCreateTable(CreateTable node, Context context) {
            context.analysis = new CreateTableAnalysis(referenceInfos, fulltextAnalyzerResolver, context.parameters);
            return createTableStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitCreateAnalyzer(CreateAnalyzer node, Context context) {
            context.analysis = new CreateAnalyzerAnalysis(fulltextAnalyzerResolver, context.parameters);
            return createAnalyzerStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitCreateBlobTable(CreateBlobTable node, Context context) {
            context.analysis = new CreateBlobTableAnalysis(context.parameters);
            return createBlobTableStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitDropBlobTable(DropBlobTable node, Context context) {
            context.analysis = new DropBlobTableAnalysis(context.parameters, referenceInfos);
            return dropBlobTableStatementAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitAlterBlobTable(AlterBlobTable node, Context context) {
            context.analysis = new AlterBlobTableAnalysis(context.parameters, referenceInfos);
            return alterBlobTableAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitRefreshStatement(RefreshStatement node, Context context) {
            context.analysis = new RefreshTableAnalysis(referenceInfos, context.parameters);
            return refreshTableAnalyzer;
        }

        @Override
        public AbstractStatementAnalyzer visitAlterTable(AlterTable node, Context context) {
            context.analysis = new AlterTableAnalysis(context.parameters, referenceInfos);
            return alterTableAnalyzer;
        }

        @Override
        protected AbstractStatementAnalyzer visitNode(Node node, Context context) {
            throw new UnsupportedOperationException(String.format("cannot analyze statement: '%s'", node));
        }
    }
}
