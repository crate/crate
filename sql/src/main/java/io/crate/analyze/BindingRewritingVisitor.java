package io.crate.analyze;

import io.crate.analyze.tree.BoundFunctionCall;
import io.crate.analyze.tree.BoundReference;
import io.crate.metadata.ReferenceInfo;
import io.crate.sql.tree.*;

public class BindingRewritingVisitor extends AstVisitor<Expression, BindingContext> {

    @Override
    protected Expression visitSingleColumn(SingleColumn node, BindingContext context) {
        if (!node.getAlias().isPresent()) {
            // TODO: toString() repr is not always correct
            node.setAlias(node.getExpression().toString());
        }
        node.setExpression(node.getExpression().accept(this, context));
        return null;
    }

    @Override
    protected BoundReference visitSubscriptExpression(SubscriptExpression node, BindingContext context) {
        ReferenceInfo info = context.getReferenceInfo(node);
        return new BoundReference(info);
    }

    @Override
    protected BoundFunctionCall visitFunctionCall(FunctionCall node, BindingContext context) {
        // TODO: need to replace expressions from node.getArguments()
        return new BoundFunctionCall(context.getFunctionInfo(node), node.getArguments());
    }
}
