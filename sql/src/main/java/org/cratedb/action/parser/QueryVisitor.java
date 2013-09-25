package org.cratedb.action.parser;

import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.sql.SQLParseException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * The QueryVisitor is an implementation of the Visitor interface provided by the akiban SQL-Parser
 * See https://github.com/akiban/sql-parser for more information.
 *
 */
public class QueryVisitor implements XContentVisitor {

    private XContentGenerator generator = null;
    private boolean stopTraverse;

    public QueryVisitor(NodeExecutionContext executionContext) throws StandardException {
        generator = new XContentGenerator(executionContext, new Object[0]);
        stopTraverse = false;
    }

    public QueryVisitor(NodeExecutionContext executionContext, Object[] args) throws StandardException {
        generator = new XContentGenerator(executionContext, args);
        stopTraverse = false;
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

    public List<String> getIndices() {
        return generator.getIndices();
    }

    /**
     * See {@link org.cratedb.action.parser.XContentGenerator#outputFields()}
     * @return
     */
    public List<Tuple<String, String>> outputFields() {
        return generator.outputFields();
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
