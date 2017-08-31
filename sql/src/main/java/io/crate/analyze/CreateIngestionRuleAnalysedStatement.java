package io.crate.analyze;

import io.crate.metadata.TableIdent;
import io.crate.sql.tree.Expression;

import javax.annotation.Nullable;

public class CreateIngestionRuleAnalysedStatement implements DCLStatement {
    private final String ruleName;
    private final String sourceName;
    private final TableIdent targetTable;
    private final Expression whereClause;
    private final ParameterContext parameterContext;

    public CreateIngestionRuleAnalysedStatement(String ruleName,
                                                String sourceName,
                                                TableIdent targetTable,
                                                @Nullable  Expression whereClause,
                                                ParameterContext parameterContext) {
        this.ruleName = ruleName;
        this.sourceName = sourceName;
        this.targetTable = targetTable;
        this.whereClause = whereClause;
        this.parameterContext = parameterContext;
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

    @Nullable
    public Expression whereClause() {
        return whereClause;
    }

    public ParameterContext parameterContext() {
        return parameterContext;
    }
}
