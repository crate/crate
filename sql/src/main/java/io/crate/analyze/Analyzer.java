package io.crate.analyze;

import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.ReferenceResolver;
import io.crate.sql.tree.*;
import org.elasticsearch.common.inject.Inject;

public class Analyzer {

    private final ReferenceInfos referenceInfos;
    private final Functions functions;
    private final ReferenceResolver referenceResolver;
    private final StatementAnalyzer selectStatementAnalyzer = new SelectStatementAnalyzer();
    private final StatementAnalyzer insertStatementAnalyzer = new InsertStatementAnalyzer();
    private final StatementAnalyzer updateStatementAnalyzer = new UpdateStatementAnalyzer();
    private final StatementAnalyzer deleteStatementAnalyzer = new DeleteStatementAnalyzer();
    private final StatementAnalyzer copyStatementAnalyzer = new CopyStatementAnalyzer();

    @Inject
    public Analyzer(ReferenceInfos referenceInfos, Functions functions, ReferenceResolver referenceResolver) {
        this.referenceInfos = referenceInfos;
        this.functions = functions;
        this.referenceResolver = referenceResolver;
    }

    public Analysis analyze(Statement statement) {
        return analyze(statement, new Object[0]);
    }

    public Analysis analyze(Statement statement, Object[] parameters) {
        Analysis analysis;
        StatementAnalyzer statementAnalyzer;

        if (statement instanceof Query) {
            analysis = new SelectAnalysis(referenceInfos, functions, parameters, referenceResolver);
            statementAnalyzer = selectStatementAnalyzer;
        } else if (statement instanceof Delete) {
            analysis = new DeleteAnalysis(referenceInfos, functions, parameters, referenceResolver);
            statementAnalyzer = deleteStatementAnalyzer;
        } else if (statement instanceof Insert) {
            statementAnalyzer = insertStatementAnalyzer;
            analysis = new InsertAnalysis(referenceInfos, functions, parameters, referenceResolver);
        } else if (statement instanceof Update) {
            statementAnalyzer = updateStatementAnalyzer;
            analysis = new UpdateAnalysis(referenceInfos, functions, parameters, referenceResolver);
        } else if (statement instanceof CopyFromStatement) {
            statementAnalyzer = copyStatementAnalyzer;
            analysis = new CopyAnalysis(referenceInfos, functions, parameters, referenceResolver);
        } else {
            throw new UnsupportedOperationException(String.format("cannot analyze statement: '%s'", statement));
        }
        statement.accept(statementAnalyzer, analysis);
        analysis.normalize();
        return analysis;
    }

}
