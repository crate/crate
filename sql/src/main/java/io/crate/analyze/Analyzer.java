package io.crate.analyze;

import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfos;
import io.crate.sql.tree.Delete;
import io.crate.sql.tree.Insert;
import io.crate.sql.tree.Query;
import io.crate.sql.tree.Statement;
import org.elasticsearch.common.inject.Inject;

public class Analyzer {

    private final ReferenceInfos referenceInfos;
    private final Functions functions;
    private final StatementAnalyzer selectStatementAnalyzer = new SelectStatementAnalyzer();
    private final StatementAnalyzer insertStatementAnalyzer = new InsertStatementAnalyzer();

    @Inject
    public Analyzer(ReferenceInfos referenceInfos, Functions functions) {
        this.referenceInfos = referenceInfos;
        this.functions = functions;
    }

    public Analysis analyze(Statement statement) {
        return analyze(statement, new Object[0]);
    }

    public Analysis analyze(Statement statement, Object[] parameters) {
        Analysis analysis;
        StatementAnalyzer statementAnalyzer;

        if (statement instanceof Query || statement instanceof Delete) {
            analysis = new SelectAnalysis(referenceInfos, functions, parameters);
            statementAnalyzer = selectStatementAnalyzer;
        } else if (statement instanceof Insert) {
            statementAnalyzer = insertStatementAnalyzer;
            analysis = new InsertAnalysis(referenceInfos, functions, parameters);
        } else {
            throw new UnsupportedOperationException(String.format("cannot analyze statement: '%s'", statement));
        }
        statement.accept(statementAnalyzer, analysis);
        return analysis;
    }

}
