package org.cratedb.action.parser;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.sql.SQLParseException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The QueryVisitor is an implementation of the Visitor interface provided by the SQL-Parser
 *
 */
public class QueryVisitor extends XContentVisitor {

    private XContentGenerator generator = null;
    private boolean stopTraverse;
    private NodeExecutionContext.TableExecutionContext tableContext;

    public QueryVisitor(ParsedStatement stmt) throws StandardException {
        super(stmt);
        generator = new XContentGenerator(stmt);
        stopTraverse = false;
    }


    private Object getMappedValue(
            NodeExecutionContext.TableExecutionContext tc,
            ValueNode value,
            String columnName){

        return tc.mapper().mappers().name(columnName).mapper().value(value);
    }

    private void setTable(String tableName){
        stmt.addIndex(tableName);
        tableContext = stmt.context().tableContext(tableName);
    }

    public Visitable visit(UpdateNode node) throws StandardException {
        String tableName = node.getTargetTableName().getTableName();
        setTable(tableName);
        try {
            generator.generate(node);
        } catch (IOException ex) {
            throw new StandardException(ex);
        }

        // TODO: merge docs
        Map<String, Object> updateDoc = new HashMap<String, Object>();
        for (ResultColumn rc: ((SelectNode)node.getResultSetNode()).getResultColumns()){
            String key = rc.getName();
            Object value = evaluateValueNode(tableContext, key, rc.getExpression());
            updateDoc.put(key, value);
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
        switch (treeNode.getNodeType()) {
            case NodeTypes.CURSOR_NODE:
                stopTraverse = true;
                return visit((CursorNode)node);
            case NodeTypes.UPDATE_NODE:
                stopTraverse = true;
                return visit((UpdateNode)node);
            case NodeTypes.DELETE_NODE:
                stopTraverse = true;
                return visit((DeleteNode)node);
            default:
                throw new SQLParseException(
                    "First node wasn't a CURSOR_NODE or DELETE_NODE. Unsupported Statement");
        }
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
