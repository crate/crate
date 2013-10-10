package org.cratedb.action.parser;

import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

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

        Map<String, Object> updateDoc = new HashMap<String, Object>();
        for (ResultColumn rc: (node.getResultSetNode()).getResultColumns()){
            if (rc.getReference() != null && rc.getReference() instanceof NestedColumnReference) {
                // nested object column (in format col1[prop1][prop2]...
                NestedColumnReference nestedColumn = (NestedColumnReference)rc.getReference();

                // [col1, prop1, prop2]
                String[] path = toNestedColumnPath(rc.getName(), nestedColumn);

                // to get the correct value from the fieldMapper the key has to be in the format of
                // col1.prop1.prop2
                String fullKey = nestedColumn.xcontentPathString();
                Object value = evaluateValueNode(tableContext, fullKey, rc.getExpression());

                addValue(updateDoc, path, value, 0);
            } else {
                addValue(updateDoc, new String[] { rc.getName() },
                    evaluateValueNode(tableContext, rc.getName(), rc.getExpression()), 0
                );
            }
        }

        stmt.updateDoc(updateDoc);
        return node;
    }

    /**
     * merge newValue into the updateDoc
     * will call itself recursive to walk down the path
     *
     * @param updateDoc
     * @param path flat path to the property inside the map
     *             e.g. [col1, prop1, prop2] in case of col1[prop1][prop2]
     * @param newValue the value that should be assigned to the property specified with path/idx
     *                 e.g. col1[prop1][prop2] = newValue
     * @param idx current position of path
     *            should always be 0, will call itself recursive to walk down the path
     */
    private void addValue(Map<String,Object> updateDoc, String[] path, Object newValue, int idx) {
        String elem = path[idx];
        Object value = updateDoc.get(elem);

        if (value == null) {
            if (idx + 1 == path.length) {
                updateDoc.put(elem, newValue);
            } else {
                Map<String, Object> subMap = newHashMap();
                addValue(subMap, path, newValue, idx + 1);
                updateDoc.put(elem, subMap);
            }
        } else if (value instanceof Map) {
            Map<String, Object> valueMap = (Map<String, Object>)value;
            addValue(valueMap, path, newValue, idx + 1);
        } else {
            updateDoc.put(elem, newValue);
        }
    }

    private String[] toNestedColumnPath(String columnName, NestedColumnReference nestedColumn)
        throws StandardException
    {
        String[] paths = new String[nestedColumn.path().size() + 1];
        paths[0] = columnName;

        for (int i = 0; i < nestedColumn.path().size(); i++) {
            ValueNode node = nestedColumn.path().get(i);
            if (node instanceof CharConstantNode) {
                paths[i + 1] =  ((CharConstantNode)node).getString();
            } else {
                // e.g. col1[0]  (numeric index access) is currently not supported.
                throw new StandardException(
                    "Unexcpected ValueNode class inside NestedColumnReference path");
            }
        }

        return paths;
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
        String tableName = node.getTargetTableName().getTableName();
        setTable(tableName);
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
