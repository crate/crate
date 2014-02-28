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
import org.elasticsearch.common.inject.Inject;

public class Analyzer {

    private final AnalyzerDispatcher dispatcher;

    @Inject
    public Analyzer(ReferenceInfos referenceInfos, Functions functions, ReferenceResolver referenceResolver) {
        this.dispatcher = new AnalyzerDispatcher(referenceInfos, functions, referenceResolver);
    }

    public Analysis analyze(Statement statement) {
        return analyze(statement, new Object[0]);
    }

    public Analysis analyze(Statement statement, Object[] parameters) {
        DataStatementAnalyzer statementAnalyzer;

        Context ctx = new Context(parameters);
        statementAnalyzer = dispatcher.process(statement, ctx);
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

    private static class AnalyzerDispatcher extends AstVisitor<DataStatementAnalyzer, Context> {

        private final ReferenceInfos referenceInfos;
        private final Functions functions;
        private final ReferenceResolver referenceResolver;

        private final DataStatementAnalyzer selectStatementAnalyzer = new SelectStatementAnalyzer();
        private final DataStatementAnalyzer insertStatementAnalyzer = new InsertStatementAnalyzer();
        private final DataStatementAnalyzer updateStatementAnalyzer = new UpdateStatementAnalyzer();
        private final DataStatementAnalyzer deleteStatementAnalyzer = new DeleteStatementAnalyzer();
        private final DataStatementAnalyzer copyStatementAnalyzer = new CopyStatementAnalyzer();
        //private final StatementAnalyzer createTableStatementAnalyzer = new CreateTableStatementAnalyzer();

        public AnalyzerDispatcher(ReferenceInfos referenceInfos,
                                  Functions functions,
                                  ReferenceResolver referenceResolver) {
            this.referenceInfos = referenceInfos;
            this.functions = functions;
            this.referenceResolver = referenceResolver;
        }

        @Override
        protected DataStatementAnalyzer visitQuery(Query node, Context context) {
            context.analysis = new SelectAnalysis(
                    referenceInfos, functions, context.parameters, referenceResolver);
            return selectStatementAnalyzer;
        }

        @Override
        public DataStatementAnalyzer visitDelete(Delete node, Context context) {
            context.analysis = new DeleteAnalysis(
                    referenceInfos, functions, context.parameters, referenceResolver);
            return deleteStatementAnalyzer;
        }

        @Override
        public DataStatementAnalyzer visitInsert(Insert node, Context context) {
            context.analysis = new InsertAnalysis(
                    referenceInfos, functions, context.parameters, referenceResolver);
            return insertStatementAnalyzer;
        }

        @Override
        public DataStatementAnalyzer visitUpdate(Update node, Context context) {
            context.analysis = new UpdateAnalysis(
                    referenceInfos, functions, context.parameters, referenceResolver);
            return updateStatementAnalyzer;
        }

        @Override
        public DataStatementAnalyzer visitCopyFromStatement(CopyFromStatement node, Context context) {
            context.analysis = new CopyAnalysis(
                    referenceInfos, functions, context.parameters, referenceResolver);
            return copyStatementAnalyzer;
        }

        //@Override
        //public StatementAnalyzer visitCreateTable(CreateTable node, Context context) {
        //    context.analysis = new CreateTableAnalysis();
        //    return createTableStatementAnalyzer;
        //}

        @Override
        protected DataStatementAnalyzer visitNode(Node node, Context context) {
            throw new UnsupportedOperationException(String.format("cannot analyze statement: '%s'", node));
        }
    }
}
