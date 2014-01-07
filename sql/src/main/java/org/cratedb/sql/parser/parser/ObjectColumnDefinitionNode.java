package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.types.DataTypeDescriptor;

/**
 * Crate Object Type Column Definition with optional subcolumns and an ObjectType
 * to define its behaviour
 */
public class ObjectColumnDefinitionNode extends ColumnDefinitionNode {
    private ObjectType objectType = ObjectType.DYNAMIC;
    private TableElementList subColumns = null;

    @Override
    public void init(Object name, Object objectType, Object subColumns) throws StandardException {
        super.init(name, null, DataTypeDescriptor.OBJECT, null);
        this.objectType = (ObjectType)objectType;
        this.subColumns = (TableElementList)subColumns;
    }

    public TableElementList subColumns() {
        return subColumns;
    }

    public ObjectType objectType() {
        return objectType;
    }

    @Override
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);
        ObjectColumnDefinitionNode other = (ObjectColumnDefinitionNode)node;
        this.objectType = other.objectType;
        this.subColumns = (TableElementList) getNodeFactory().copyNode(other.subColumns, getParserContext());
    }

    @Override
    public String toString() {
        return super.toString() +
                "objectType: " + objectType().name() + "\n";
    }

    @Override
    public void printSubNodes(int depth) {
        super.printSubNodes(depth);
        if (subColumns!=null) {
            printLabel(depth, "subColumns: ");
            subColumns.treePrint(depth+1);
        }
    }
}
