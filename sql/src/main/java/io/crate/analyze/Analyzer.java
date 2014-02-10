package io.crate.analyze;

import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfos;
import io.crate.sql.tree.Statement;
import org.elasticsearch.common.inject.Inject;

public class Analyzer {

    private final ReferenceInfos referenceInfos;
    private final StatementAnalyzer statementAnalyzer = new StatementAnalyzer();
    private final Functions functions;

    @Inject
    public Analyzer(ReferenceInfos referenceInfos, Functions functions) {
        this.referenceInfos = referenceInfos;
        this.functions = functions;
    }

    public Analysis analyze(Statement statement) {
        Analysis analysis = new Analysis(referenceInfos, functions);
        statement.accept(statementAnalyzer, analysis);
        return analysis;
    }

}
