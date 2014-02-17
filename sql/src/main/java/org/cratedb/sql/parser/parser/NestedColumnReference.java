/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

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
    }

    @Override
    public String getColumnName() {
        try {
            return xcontentPathString();
        } catch (StandardException e) {
            return columnName;
        }
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
     * Generates the path strings by iterating over the path elements
     *
     * @throws StandardException
     */
    private void generatePathStrings() throws StandardException {
        StringBuilder xcontentBuilder = new StringBuilder().append(columnName);
        StringBuilder sqlBuilder = new StringBuilder().append(columnName);
        for (ValueNode node : path) {
            Object value = ((CharConstantNode) node).getString();
            xcontentBuilder.append(".").append(value);
            sqlBuilder.append("['").append(value).append("']");
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

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        String nestedColumnName = columnName;
        try {
            nestedColumnName = getColumnName();
        } catch (Exception e) {}
        return "columnName: " + nestedColumnName + "\n" +
                "tableName: " + ( ( getTableName() != null) ?
                getTableName().toString() :
                "null") + "\n" +
                "type: " +
                ((getType() != null) ? getType().toString() : "null" ) + "\n"
                ;
    }

}
