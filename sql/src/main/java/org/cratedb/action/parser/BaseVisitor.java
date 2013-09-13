package org.cratedb.action.parser;


import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;

public abstract class BaseVisitor implements Visitor {

    public abstract void visit(SelectNode node);
    public abstract void visit(ValueNode parentNode, BinaryRelationalOperatorNode node);
    public abstract void visit(ValueNode parentNode, AndNode node);
    public abstract void visit(ValueNode parentNode, OrNode node);
    public abstract void visit(ValueNode parentNode, IsNullNode node);

    @Override
    public Visitable visit(Visitable node) throws StandardException {
        switch (((QueryTreeNode)node).getNodeType()) {
            case NodeTypes.SELECT_NODE:
                visit((SelectNode)node);
                break;
            }

        return node;
    }

    protected void visit(ValueNode parentNode, ValueNode node) {
        switch (node.getNodeType()) {
            case NodeTypes.OR_NODE:
                visit(parentNode, (OrNode)node);
                return;
            case NodeTypes.AND_NODE:
                visit(parentNode, (AndNode)node);
                return;
            case NodeTypes.IS_NULL_NODE:
                visit(parentNode, (IsNullNode)node);
                return;
        }

        if (node instanceof BinaryRelationalOperatorNode) {
            visit(parentNode, (BinaryRelationalOperatorNode) node);
        }
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return false;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        return false;
    }
}
