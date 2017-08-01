package io.crate.analyze;

import io.crate.metadata.TableIdent;

public class CreateIngestionRuleAnalysedStatement implements DCLStatement {
    private final String ruleName;
    private final String sourceName;
    private final TableIdent targetTable;
    private final String whereClause;

    public CreateIngestionRuleAnalysedStatement(String ruleName,
                                                String sourceName,
                                                TableIdent targetTable,
                                                String whereClause) {
        this.ruleName = ruleName;
        this.sourceName = sourceName;
        this.targetTable = targetTable;
        this.whereClause = whereClause;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitCreateIngestRuleStatement(this, context);
    }

    public String ruleName() {
        return ruleName;
    }

    public String sourceName() {
        return sourceName;
    }

    public TableIdent targetTable() {
        return targetTable;
    }

    public String whereClause() {
        return whereClause;
    }
}
