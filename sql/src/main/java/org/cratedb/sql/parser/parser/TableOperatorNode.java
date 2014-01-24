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

/* The original from which this derives bore the following: */

package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;

/**
 * A TableOperatorNode represents a relational operator like UNION, INTERSECT,
 * JOIN, etc. that takes two tables as parameters and returns a table.  The
 * parameters it takes are represented as ResultSetNodes.
 *
 * Currently, all known table operators are binary operators, so there are no
 * subclasses of this node type called "BinaryTableOperatorNode" and
 * "UnaryTableOperatorNode".
 *
 */

abstract class TableOperatorNode extends FromTable
{
    protected ResultSetNode leftResultSet;
    protected ResultSetNode rightResultSet;

    /**
     * Initializer for a TableOperatorNode.
     *
     * @param leftResultSet The ResultSetNode on the left side of this node
     * @param rightResultSet The ResultSetNode on the right side of this node
     * @param tableProperties Properties list associated with the table
     *
     * @exception StandardException Thrown on error
     */
    public void init(Object leftResultSet,
                     Object rightResultSet,
                     Object tableProperties)
            throws StandardException {
        /* correlationName is always null */
        init(null, tableProperties);
        this.leftResultSet = (ResultSetNode)leftResultSet;
        this.rightResultSet = (ResultSetNode)rightResultSet;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        TableOperatorNode other = (TableOperatorNode)node;
        this.leftResultSet = (ResultSetNode)getNodeFactory().copyNode(other.leftResultSet,
                                                                      getParserContext());
        this.rightResultSet = (ResultSetNode)getNodeFactory().copyNode(other.rightResultSet,
                                                                       getParserContext());
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return "nestedInParens: " + false + "\n" +
            super.toString();
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        super.printSubNodes(depth);

        if (leftResultSet != null) {
            printLabel(depth, "leftResultSet: ");
            leftResultSet.treePrint(depth + 1);
        }

        if (rightResultSet != null) {
            printLabel(depth, "rightResultSet: ");
            rightResultSet.treePrint(depth + 1);
        }
    }

    /**
     * Get the leftResultSet from this node.
     *
     * @return ResultSetNode The leftResultSet from this node.
     */
    public ResultSetNode getLeftResultSet() {
        return leftResultSet;
    }

    /**
     * Get the rightResultSet from this node.
     *
     * @return ResultSetNode The rightResultSet from this node.
     */
    public ResultSetNode getRightResultSet() {
        return rightResultSet;
    }

    public void setLeftResultSet(ResultSetNode leftResultSet) {
        this.leftResultSet =    leftResultSet;
    }

    public void setRightResultSet(ResultSetNode rightResultSet) {
        this.rightResultSet =    rightResultSet;
    }

    public ResultSetNode getLeftmostResultSet() {
        if (leftResultSet instanceof TableOperatorNode) {
            return ((TableOperatorNode)leftResultSet).getLeftmostResultSet();
        }
        else {
            return leftResultSet;
        }
    }

    public void setLeftmostResultSet(ResultSetNode newLeftResultSet) {
        if (leftResultSet instanceof TableOperatorNode) {
            ((TableOperatorNode)leftResultSet).setLeftmostResultSet(newLeftResultSet);
        }
        else {
            this.leftResultSet = newLeftResultSet;
        }
    }

    /**
     * Return the exposed name for this table, which is the name that
     * can be used to refer to this table in the rest of the query.
     *
     * @return The exposed name for this table.
     */

    public String getExposedName() {
        return null;
    }

    /**
     * Mark whether or not this node is nested in parens.    (Useful to parser
     * since some trees get created left deep and others right deep.)
     * The resulting state of this cal was never used so its
     * field was removed to save runtimespace for this node.
     * Further cleanup can be done including parser changes
     * if this call is really nor required.
     *
     * @param nestedInParens Whether or not this node is nested in parens.
     */
    public void setNestedInParens(boolean nestedInParens) {
    }

    /**
     * Accept the visitor for all visitable children of this node.
     * 
     * @param v the visitor
     *
     * @exception StandardException on error
     */
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        if (leftResultSet != null) {
            leftResultSet = (ResultSetNode)leftResultSet.accept(v);
        }
        if (rightResultSet != null) {
            rightResultSet = (ResultSetNode)rightResultSet.accept(v);
        }
    }

}
