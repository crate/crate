package io.crate.analyze;

public class DropIngestionRuleAnalysedStatement implements DCLStatement {
    private final String ruleName;
    private final boolean ifExists;

    public DropIngestionRuleAnalysedStatement(String ruleName, boolean ifExists) {
        this.ruleName = ruleName;
        this.ifExists = ifExists;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitDropIngestRuleStatement(this, context);
    }

    public String ruleName() {
        return ruleName;
    }

    public boolean ifExists() {
        return ifExists;
    }
}
