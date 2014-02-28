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
        StatementAnalyzer statementAnalyzer;

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

    private static class AnalyzerDispatcher extends AstVisitor<StatementAnalyzer, Context> {

        private final ReferenceInfos referenceInfos;
        private final Functions functions;
        private final ReferenceResolver referenceResolver;

        private final StatementAnalyzer selectStatementAnalyzer = new SelectStatementAnalyzer();
        private final StatementAnalyzer insertStatementAnalyzer = new InsertStatementAnalyzer();
        private final StatementAnalyzer updateStatementAnalyzer = new UpdateStatementAnalyzer();
        private final StatementAnalyzer deleteStatementAnalyzer = new DeleteStatementAnalyzer();
        private final StatementAnalyzer copyStatementAnalyzer = new CopyStatementAnalyzer();
        //private final StatementAnalyzer createTableStatementAnalyzer = new CreateTableStatementAnalyzer();

        public AnalyzerDispatcher(ReferenceInfos referenceInfos,
                                  Functions functions,
                                  ReferenceResolver referenceResolver) {
            this.referenceInfos = referenceInfos;
            this.functions = functions;
            this.referenceResolver = referenceResolver;
        }

        @Override
        protected StatementAnalyzer visitQuery(Query node, Context context) {
            context.analysis = new SelectAnalysis(
                    referenceInfos, functions, context.parameters, referenceResolver);
            return selectStatementAnalyzer;
        }

        @Override
        public StatementAnalyzer visitDelete(Delete node, Context context) {
            context.analysis = new DeleteAnalysis(
                    referenceInfos, functions, context.parameters, referenceResolver);
            return deleteStatementAnalyzer;
        }

        @Override
        public StatementAnalyzer visitInsert(Insert node, Context context) {
            context.analysis = new InsertAnalysis(
                    referenceInfos, functions, context.parameters, referenceResolver);
            return insertStatementAnalyzer;
        }

        @Override
        public StatementAnalyzer visitUpdate(Update node, Context context) {
            context.analysis = new UpdateAnalysis(
                    referenceInfos, functions, context.parameters, referenceResolver);
            return updateStatementAnalyzer;
        }

        @Override
        public StatementAnalyzer visitCopyFromStatement(CopyFromStatement node, Context context) {
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
        protected StatementAnalyzer visitNode(Node node, Context context) {
            throw new UnsupportedOperationException(String.format("cannot analyze statement: '%s'", node));
        }
    }
}
