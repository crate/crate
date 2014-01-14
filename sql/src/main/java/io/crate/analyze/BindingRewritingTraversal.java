package io.crate.analyze;

import io.crate.sql.tree.*;

public class BindingRewritingTraversal extends AstVisitor<Expression, BindingContext> {

    private final AstVisitor<Expression, BindingContext> rewriter;

    public BindingRewritingTraversal(AstVisitor<Expression, BindingContext> rewriter) {
        this.rewriter = rewriter;
    }

    @Override
    protected Expression visitQuery(Query node, BindingContext context) {
        process(node.getQueryBody(), context);
        return null;
    }

    @Override
    protected Expression visitQuerySpecification(QuerySpecification node, BindingContext context) {
        process(node.getSelect(), context);
        if (node.getFrom() != null) {
            for (Relation relation : node.getFrom()) {
                process(relation, context);
            }
        }

        if (node.getWhere().isPresent()) {
            process(node.getWhere().get(), context);
        }

        for (Expression expression : node.getGroupBy()) {
            process(expression, context);
        }

        if (node.getHaving().isPresent()) {
            process(node.getHaving().get(), context);
        }

        for (SortItem sortItem : node.getOrderBy()) {
            process(sortItem, context);
        }

        return null;
    }

    @Override
    protected Expression visitSelect(Select node, BindingContext context) {
        for (SelectItem item : node.getSelectItems()) {
            process(item, context);
        }
        return null;
    }

    @Override
    protected Expression visitSingleColumn(SingleColumn node, BindingContext context) {
        node.accept(rewriter, context);
        return null;
    }

    @Override
    protected Expression visitAllColumns(AllColumns node, BindingContext context) {
        return super.visitAllColumns(node, context);
    }
}
