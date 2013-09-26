package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;

import java.util.ArrayList;
import java.util.List;

/**
 * A NestedColumnReference represents a column in the query tree with an nested object
 * expression.
 * See ColumnReference.java for general column reference description.
 */
public class NestedColumnReference extends ColumnReference {

    private List<ValueNode> path;
    private String sqlPathString;
    private String xcontentPathString;
    private Boolean pathContainsNumeric;

    /**
     * Initializer.
     * This one is called by the parser where we could
     * be dealing with delimited identifiers.
     *
     * @param columnName The name of the column being referenced
     * @param tableName The qualification for the column
     * @param tokBeginOffset begin position of token for the column name
     *              identifier from parser.
     * @param tokEndOffset position of token for the column name
     *              identifier from parser.
     */

    public void init(Object columnName,
                     Object tableName,
                     Object tokBeginOffset,
                     Object tokEndOffset) {
        super.init(columnName, tableName, tokBeginOffset, tokEndOffset);
        path = new ArrayList<ValueNode>();
        sqlPathString = null;
        xcontentPathString = null;
        pathContainsNumeric = false;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        NestedColumnReference other = (NestedColumnReference)node;
        this.path = other.path;
    }

    /**
     * Returns the path elements
     *
     * @return
     */
    public List<ValueNode> path() {
        return path;
    }

    /**
     * Adds a node to the path list
     *
     * @param node
     */
    public void addPathElement(ValueNode node) {
        path.add(node);
    }


    /**
     * Returns the full column sql path expression
     *
     * @return
     * @throws StandardException
     */
    public String sqlPathString() throws StandardException {
        if (sqlPathString == null) {
            generatePathStrings();
        }
        return sqlPathString;
    }

    /**
     * Returns the full column path expression translated for use in XContent
     *
     * @return
     * @throws StandardException
     */
    public String xcontentPathString() throws StandardException {
        if (xcontentPathString == null) {
            generatePathStrings();
        }
        return xcontentPathString;
    }

    /**
     * Whether the path contains a NumericConstantNode (array index)
     *
     * @return
     * @throws StandardException
     */
    public Boolean pathContainsNumeric() throws StandardException {
        if (sqlPathString == null || xcontentPathString == null) {
            generatePathStrings();
        }
        return pathContainsNumeric;
    }

    /**
     * Generates the path strings through iterating over the path elements
     *
     * @throws StandardException
     */
    private void generatePathStrings() throws StandardException {
        StringBuilder xcontentBuilder = new StringBuilder().append(getColumnName());
        StringBuilder sqlBuilder = new StringBuilder().append(getColumnName());
        for (ValueNode node : path) {
            Object value = null;
            if (node instanceof CharConstantNode) {
                value = ((CharConstantNode) node).getString();
                xcontentBuilder.append(".").append(value);
                sqlBuilder.append("['").append(value).append("']");
            } else if (node instanceof NumericConstantNode) {
                value = ((NumericConstantNode) node).getValue();
                xcontentBuilder.append("[").append(value).append("]");
                sqlBuilder.append("[").append(value).append("]");
                pathContainsNumeric = true;
            } else {
                throw new StandardException("Only indexes or property names can " +
                        "currently be used for nested column references");
            }
        }
        sqlPathString = sqlBuilder.toString();
        xcontentPathString = xcontentBuilder.toString();
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        super.printSubNodes(depth);
        if (path != null) {
            printLabel(depth, "path:\n");
            for (int index = 0; index < path.size(); index++) {
                debugPrint(formatNodeString("[" + index + "]:", depth+1));
                ValueNode row = path.get(index);
                row.treePrint(depth+1);
            }
        }
    }

}
