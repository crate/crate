package org.cratedb.action.collect.scope;

import org.cratedb.DataType;
import org.cratedb.action.parser.ColumnDescription;

public class GlobalExpressionDescription extends ColumnDescription {

    private final String name;
    private final ExpressionScope scope;
    private final DataType returnType;

    public GlobalExpressionDescription(ScopedExpression<?> expression) {
        super(Types.CONSTANT_COLUMN);
        this.name = expression.name();
        this.scope = expression.getScope();
        this.returnType = expression.returnType();
    }

    public ExpressionScope scope() {
        return scope;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public DataType returnType() {
        return returnType;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + scope.hashCode();
        result = 31 * result + returnType.hashCode();
        return result;
    }
}
