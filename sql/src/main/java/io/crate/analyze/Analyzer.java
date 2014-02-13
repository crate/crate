package io.crate.analyze;

import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.ReferenceResolver;
import io.crate.sql.tree.Statement;
import org.elasticsearch.common.inject.Inject;

public class Analyzer {

    private final ReferenceInfos referenceInfos;
    private final StatementAnalyzer statementAnalyzer = new StatementAnalyzer();
    private final Functions functions;
    private final ReferenceResolver referenceResolver;

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
        Analysis analysis = new Analysis(referenceInfos, functions, parameters, referenceResolver);
        statement.accept(statementAnalyzer, analysis);
        return analysis;
    }

}
