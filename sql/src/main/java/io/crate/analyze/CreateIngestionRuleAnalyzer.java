package io.crate.analyze;


import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.sql.ExpressionFormatter;
import io.crate.sql.tree.CreateIngestRule;
import io.crate.sql.tree.ParameterExpression;


public class CreateIngestionRuleAnalyzer {

    private static class ExpressionParameterSubstitutionFormatter extends ExpressionFormatter.Formatter {

        private final Analysis analysis;

        ExpressionParameterSubstitutionFormatter(Analysis context) {
            this.analysis = context;
        }

        @Override
        public String visitParameterExpression(ParameterExpression node, Void context) {
            String formattedExpression = null;
            Symbol symbol = analysis.parameterContext().apply(node);
            if (symbol != null) {
                formattedExpression = symbol.representation();
            }
            return formattedExpression;
        }
    }

    private final Schemas schemas;

    CreateIngestionRuleAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    CreateIngestionRuleAnalysedStatement analyze(CreateIngestRule node, Analysis context) {
        TableIdent tableIdent = TableIdent.of(node.targetTable(), Schemas.DEFAULT_SCHEMA_NAME);
        ensureTableExists(tableIdent);

        String whereCondition = "";
        if (node.where().isPresent()) {
            whereCondition = ExpressionFormatter.formatStandaloneExpression(node.where().get(),
                new ExpressionParameterSubstitutionFormatter(context));
        }

        return new CreateIngestionRuleAnalysedStatement(node.ruleName(),
            node.sourceIdent(),
            tableIdent,
            whereCondition);
    }

    private void ensureTableExists(TableIdent tableIdent) {
        schemas.getTableInfo(tableIdent);
    }
}
