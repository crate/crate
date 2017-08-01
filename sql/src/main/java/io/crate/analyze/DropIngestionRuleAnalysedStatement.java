package io.crate.analyze;

public class DropIngestionRuleAnalysedStatement implements DCLStatement {
    private final String ruleName;

    public DropIngestionRuleAnalysedStatement(String ruleName) {
        this.ruleName = ruleName;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitDropIngestRuleStatement(this, context);
    }

    public String ruleName() {
        return ruleName;
    }
}
