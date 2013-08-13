package crate.elasticsearch.action.parser;

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.*;
import crate.elasticsearch.sql.SQLParseException;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The QueryVisitor is an implementation of the Visitor interface provided by the akiban SQL-Parser
 * See https://github.com/akiban/sql-parser for more information.
 *
 */
public class QueryVisitor implements Visitor {

    private XContentGenerator generator = null;
    private boolean stopTraverse;

    public QueryVisitor() {
        generator = new XContentGenerator();
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

    public XContentBuilder getXContentBuilder() {
        return generator.getXContentBuilder();
    }

    public List<String> getIndices() {
        return generator.getIndices();
    }

    /**
     * See {@link crate.elasticsearch.action.parser.XContentGenerator#getFieldNameMapping()}
     * @return
     */
    public Map<String, String> getFieldnameMapping() {
        return generator.getFieldNameMapping();
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
            default:
                throw new SQLParseException("First node wasn't a CURSOR_NODE. Unsupported Statement");
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
