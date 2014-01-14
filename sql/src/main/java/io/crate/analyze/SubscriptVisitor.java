package io.crate.analyze;

import com.google.common.base.Preconditions;
import io.crate.sql.tree.*;


public class SubscriptVisitor extends AstVisitor<Void, SubscriptContext> {

    @Override
    protected Void visitSubscriptExpression(SubscriptExpression node, SubscriptContext context) {
        Preconditions.checkArgument(
                node.index() instanceof StringLiteral,
                "index of subscript has to be a string literal. Any other index expression is currently not supported"
        );
        Preconditions.checkArgument(
                node.name() instanceof SubscriptExpression || node.name() instanceof QualifiedNameReference,
                "An expression of type %s cannot have an index accessor ([])",
                node.getClass()
        );
        node.index().accept(this, context);
        node.name().accept(this, context);
        return null;
    }

    @Override
    protected Void visitQualifiedNameReference(QualifiedNameReference node, SubscriptContext context) {
        context.column(node.getSuffix().getSuffix());
        return null;
    }

    @Override
    protected Void visitStringLiteral(StringLiteral node, SubscriptContext context) {
        context.add(node.getValue());
        return null;
    }

    @Override
    protected Void visitExpression(Expression node, SubscriptContext context) {
        throw new UnsupportedOperationException(String.format(
                "Expression of type %s is currently not supported within a subscript expression",
                node.getClass()));
    }
}
