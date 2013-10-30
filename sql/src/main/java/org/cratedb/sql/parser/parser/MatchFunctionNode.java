package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;

public class MatchFunctionNode extends ValueNode {

    private ColumnReference columnReference;
    private ValueNode queryText;

    public void init(Object columnReference, Object queryText) {
        this.columnReference = (ColumnReference)columnReference;
        this.queryText = (ValueNode)queryText;
    }

    public ColumnReference getColumnReference() {
        return columnReference;
    }

    public ValueNode getQueryText() {
        return queryText;
    }

    @Override
    protected boolean isEquivalent(ValueNode o) throws StandardException {
        if (isSameNodeType(o)) {
            MatchFunctionNode other = (MatchFunctionNode)o;
            if (columnReference.equals(other.columnReference)
                    && queryText.equals(queryText)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void printSubNodes(int depth) {
        super.printSubNodes(depth);
        printLabel(depth, "columnReference:");
        columnReference.treePrint(depth+1);
        printLabel(depth, "queryText:");
        queryText.treePrint(depth+1);
    }

}
