package org.cratedb.sql.parser.parser;


import org.cratedb.sql.parser.StandardException;

/**
 * a node with a name and an optional list of unspecified properties.
 * Syntax:
 *
 *      name [ with (property1=value1, property2=value2, ... ) ]
 */
public class NamedNodeWithOptionalProperties extends QueryTreeNode {

    private String name;
    private GenericProperties properties = null;

    @Override
    public void init(Object name) throws StandardException {
        this.name = (String)name;
    }

    @Override
    public void init(Object name, Object properties) throws StandardException {
        this.name = (String)name;
        this.properties = (GenericProperties) properties;
    }

    public String getName() {
        return name;
    }

    public GenericProperties getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return this.name + super.toString();
    }

    @Override
    public void printSubNodes(int depth) {
        super.printSubNodes(depth);
        printLabel(depth, "Properties: ");
        if (properties != null) {
            properties.treePrint(depth+1);
        } else {
            debugPrint(formatNodeString("null", depth+1));
        }
    }

    @Override
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        properties.accept(v);
    }
}
