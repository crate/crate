package io.crate.analyze;

import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.Routings;
import io.crate.metadata.RoutingsService;
import io.crate.sql.tree.Statement;
import org.elasticsearch.common.inject.Inject;

public class Analyzer {

    private final ReferenceResolver referenceResolver;
    private final StatementAnalyzer statementAnalyzer = new StatementAnalyzer();
    private final Functions functions;
    private final Routings routings;

    @Inject
    public Analyzer(ReferenceResolver referenceResolver, Functions functions, Routings routings) {
        this.referenceResolver = referenceResolver;
        this.functions = functions;
        this.routings = routings;
    }

    public Analysis analyze(Statement statement) {
        Analysis analysis = new Analysis(referenceResolver, functions, routings);
        statement.accept(statementAnalyzer, analysis);
        return analysis;
    }

    public ReferenceResolver referenceResolver() {
        return referenceResolver;
    }

    public Functions functions() {
        return functions;
    }

    public Routings routings() {
        return routings;
    }
}
