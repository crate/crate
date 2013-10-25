package org.cratedb.action.parser;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.TableExecutionContext;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The QueryVisitor is an implementation of the Visitor interface provided by the SQL-Parser
 *
 */
public class QueryVisitor extends XContentVisitor {

    private XContentGenerator generator = null;
    private boolean stopTraverse;

    public QueryVisitor(ParsedStatement stmt) throws StandardException {
        super(stmt);
        generator = new XContentGenerator(stmt);
        stopTraverse = false;
    }


    private Object getMappedValue(
            TableExecutionContext tc,
            ValueNode value,
            String columnName){

        return tc.mapper().mappers().name(columnName).mapper().value(value);
    }

    private void setTable(String tableName){
        stmt.addIndex(tableName);
    }

    public Visitable visit(UpdateNode node) throws StandardException {
        String tableName = node.getTargetTableName().getTableName();
        setTable(tableName);
        try {
            generator.generate(node);
        } catch (IOException ex) {
            throw new StandardException(ex);
        }

        Map<String, Object> updateDoc = new HashMap<>();
        for (ResultColumn rc: (node.getResultSetNode()).getResultColumns()){
            String columnName = rc.getName();
            if (rc.getReference() != null && rc.getReference() instanceof NestedColumnReference) {
                NestedColumnReference nestedColumn = (NestedColumnReference)rc.getReference();
                if (nestedColumn.pathContainsNumeric()) {
                    throw new StandardException(
                            "Unexcpected ValueNode class inside NestedColumnReference path");
                }
                columnName = nestedColumn.xcontentPathString();
            }
            updateDoc.put(columnName, evaluateValueNode(columnName, rc.getExpression()));
        }

        stmt.updateDoc(updateDoc);
        return node;
    }
    public Visitable visit(CursorNode node) throws StandardException {
        try {
            generator.generate(node);
        } catch (IOException ex) {
            throw new StandardException(ex);
        }
        return node;
    }

    public Visitable visit(DeleteNode node) throws StandardException {
        try {
            generator.generate(node);
        } catch (IOException ex) {
            throw new StandardException(ex);
        }
        return node;
    }

    public XContentBuilder getXContentBuilder() throws StandardException {
        return generator.getXContentBuilder();
    }

    @Override
    public Visitable visit(Visitable node) throws StandardException {

        /**
         * only the CursorNode is interesting here.
         * The CursorNode can be used to traverse down the Tree as it contains the references needed.
         * This is done in the XContentGenerator.
         */
        QueryTreeNode treeNode = (QueryTreeNode)node;
        Visitable visited;
        switch (treeNode.getNodeType()) {
            case NodeTypes.CURSOR_NODE:
                stopTraverse = true;
                visited = visit((CursorNode)node);
                break;
            case NodeTypes.UPDATE_NODE:
                stopTraverse = true;
                visited = visit((UpdateNode)node);
                break;
            case NodeTypes.DELETE_NODE:
                stopTraverse = true;
                visited = visit((DeleteNode)node);
                break;
            default:
                throw new SQLParseException(
                    "First node wasn't a CURSOR_NODE or DELETE_NODE. Unsupported Statement");
        }
        // take final steps after statement has been fully parsed
        stmt.context().queryPlanner().finalizeWhereClause(stmt);
        return visited;
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return stopTraverse;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        return false;
    }
}
