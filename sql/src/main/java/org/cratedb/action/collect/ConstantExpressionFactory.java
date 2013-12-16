package org.cratedb.action.collect;

import org.cratedb.DataType;
import org.cratedb.action.sql.ITableExecutionContext;
import org.cratedb.sql.parser.parser.ValueNode;

public class ConstantExpressionFactory implements CollectorExpressionFactory {

    private static final Expression NULL_EXPRESSION = new Expression() {
        @Override
        public Object evaluate() {
            return null;
        }

        @Override
        public DataType returnType() {
            return null;
        }
    };

    public Expression create(ValueNode node, ITableExecutionContext tec) {
        return NULL_EXPRESSION;
    }

}
