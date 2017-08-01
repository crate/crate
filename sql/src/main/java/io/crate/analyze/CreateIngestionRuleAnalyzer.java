package io.crate.analyze;


import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.sql.tree.CreateIngestRule;
import io.crate.sql.tree.QualifiedName;


public class CreateIngestionRuleAnalyzer {

    private final Schemas schemas;

    public CreateIngestionRuleAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    CreateIngestionRuleAnalysedStatement analyze(CreateIngestRule node) {
        TableIdent tableIdent = TableIdent.of(node.targetTable(), Schemas.DEFAULT_SCHEMA_NAME);
        validateTableName(tableIdent);

        return new CreateIngestionRuleAnalysedStatement(node.ruleName(),
            node.sourceIdent(),
            tableIdent,
            node.where().isPresent() ? node.where().get().toString() : "");
    }

    private void validateTableName(TableIdent tableIdent) {
        schemas.getTableInfo(tableIdent);
    }
}
