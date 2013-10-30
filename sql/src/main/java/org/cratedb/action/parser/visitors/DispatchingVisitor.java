package org.cratedb.action.parser.visitors;


import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;

/**
 * Visitor that dispatches generic calls to {@link #visit(org.cratedb.sql.parser.parser.Visitable)}
 * and {@link #visit(org.cratedb.sql.parser.parser.ValueNode, org.cratedb.sql.parser.parser.ValueNode)}
 * into more specific method calls.
 *
 * E.g. visit(ValueNode, ValueNode) might become visit(ValueNode, AndNode) depending on the exact nodeType
 */
public abstract class DispatchingVisitor implements Visitor {

    protected final ParsedStatement stmt;
    protected boolean stopTraversal = false;

    protected void visit(ValueNode parentNode, BinaryRelationalOperatorNode node) throws  Exception {}
    protected void visit(ValueNode parentNode, AndNode node) throws Exception {}
    protected void visit(ValueNode parentNode, OrNode node) throws Exception {}
    protected void visit(ValueNode parentNode, IsNullNode node) throws Exception {}
    protected void visit(ValueNode parentNode, LikeEscapeOperatorNode node) throws Exception {}
    protected void visit(ValueNode parentNode, InListOperatorNode node) throws Exception {}
    protected void visit(ValueNode parentNode, NotNode node) throws Exception {}
    protected void visit(ValueNode parentNode, MatchFunctionNode node) throws Exception {}
    protected void visit(CursorNode node) throws Exception {}
    protected void visit(UpdateNode node) throws Exception {}
    protected void visit(DeleteNode node) throws Exception {}
    protected void visit(InsertNode node) throws Exception {}
    protected void visit(CreateTableNode node) throws Exception {}
    protected void visit(DropTableNode node) throws Exception {}
    protected void visit(ColumnDefinitionNode node) throws Exception {}
    protected void visit(ConstraintDefinitionNode node) throws Exception {}
    protected void visit(IndexConstraintDefinitionNode node) throws Exception {}
    protected void visit(CreateAnalyzerNode node) throws Exception {}

    protected void afterVisit() throws SQLParseException {}

    public DispatchingVisitor(ParsedStatement parsedStatement) {
        this.stmt = parsedStatement;
    }

    @Override
    public Visitable visit(Visitable node) throws StandardException {
        try {
            switch (((QueryTreeNode)node).getNodeType()) {
                case NodeTypes.CURSOR_NODE:
                    stopTraversal = true;
                    visit((CursorNode) node);
                    break;
                case NodeTypes.UPDATE_NODE:
                    stopTraversal = true;
                    visit((UpdateNode)node);
                    break;
                case NodeTypes.DELETE_NODE:
                    stopTraversal = true;
                    visit((DeleteNode)node);
                    break;
                case NodeTypes.INSERT_NODE:
                    stopTraversal = true;
                    visit((InsertNode)node);
                    break;
                case NodeTypes.CREATE_TABLE_NODE:
                    stopTraversal = true;
                    visit((CreateTableNode)node);
                    break;
                case NodeTypes.DROP_TABLE_NODE:
                    stopTraversal = true;
                    visit((DropTableNode)node);
                    break;
                case NodeTypes.CREATE_ANALYZER_NODE:
                    stopTraversal = true;
                    visit((CreateAnalyzerNode)node);
                    break;
                default:
                    throw new SQLParseException("Unsupported SQL Statement");
            }
        } catch (CrateException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new SQLParseException(ex.getMessage(), ex);
        }

        if (stopTraversal) {
            stmt.nodeType(((QueryTreeNode) node).getNodeType());
            afterVisit();
        }
        return node;
    }

    protected void visit(TableElementNode tableElement) throws Exception {
        switch(tableElement.getNodeType()) {
            case NodeTypes.COLUMN_DEFINITION_NODE:
                visit((ColumnDefinitionNode) tableElement);
                break;
            case NodeTypes.CONSTRAINT_DEFINITION_NODE:
                visit((ConstraintDefinitionNode)tableElement);
                break;
            case NodeTypes.INDEX_CONSTRAINT_NODE:
                visit((IndexConstraintDefinitionNode)tableElement);
                break;
        }
    }

    protected void visit(ValueNode parentNode, ValueNode node) throws Exception {
        switch (node.getNodeType()) {
            case NodeTypes.IN_LIST_OPERATOR_NODE:
                visit(parentNode, (InListOperatorNode)node);
                return;
            case NodeTypes.LIKE_OPERATOR_NODE:
                visit(parentNode, (LikeEscapeOperatorNode)node);
                return;
            case NodeTypes.OR_NODE:
                visit(parentNode, (OrNode)node);
                return;
            case NodeTypes.NOT_NODE:
                visit(parentNode, (NotNode)node);
                return;
            case NodeTypes.AND_NODE:
                visit(parentNode, (AndNode)node);
                return;
            case NodeTypes.IS_NULL_NODE:
                visit(parentNode, (IsNullNode)node);
                return;
            case NodeTypes.MATCH_FUNCTION_NODE:
                visit(parentNode, (MatchFunctionNode)node);
                return;
        }

        if (node instanceof BinaryRelationalOperatorNode) {
            visit(parentNode, (BinaryRelationalOperatorNode) node);
        } else {
            throw new SQLParseException("Unhandled node: " + node.toString());
        }
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return stopTraversal;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        return false;
    }
}
