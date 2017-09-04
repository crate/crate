package io.crate.analyze;


import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.sql.tree.CreateIngestRule;


public class CreateIngestionRuleAnalyzer {

    private final Schemas schemas;

    CreateIngestionRuleAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    CreateIngestionRuleAnalysedStatement analyze(CreateIngestRule node, Analysis context) {
        TableIdent tableIdent = TableIdent.of(node.targetTable(), Schemas.DOC_SCHEMA_NAME);
        ensureTableExists(tableIdent);

        return new CreateIngestionRuleAnalysedStatement(node.ruleName(),
            node.sourceIdent(),
            tableIdent,
            node.where().orElse(null),
            context.parameterContext());
    }

    private void ensureTableExists(TableIdent tableIdent) {
        schemas.getTableInfo(tableIdent);
    }
}
