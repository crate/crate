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
 * A SetOperatorNode represents a UNION, INTERSECT, or EXCEPT in a DML statement. Binding and optimization
 * preprocessing is the same for all of these operations, so they share bind methods in this abstract class.
 *
 * The class contains a boolean telling whether the operation should eliminate
 * duplicate rows.
 *
 */

public abstract class SetOperatorNode extends TableOperatorNode
{
    /**
     ** Tells whether to eliminate duplicate rows.  all == TRUE means do
     ** not eliminate duplicates, all == FALSE means eliminate duplicates.
     */
    boolean all;

    OrderByList orderByList;
    ValueNode offset; // OFFSET n ROWS
    ValueNode fetchFirst; // FETCH FIRST n ROWS ONLY

    /**
     * Initializer for a SetOperatorNode.
     *
     * @param leftResult The ResultSetNode on the left side of this union
     * @param rightResult The ResultSetNode on the right side of this union
     * @param all Whether or not this is an ALL.
     * @param tableProperties Properties list associated with the table
     *
     * @exception StandardException Thrown on error
     */

    public void init(Object leftResult,
                     Object rightResult,
                     Object all,
                     Object tableProperties)
            throws StandardException {
        super.init(leftResult, rightResult, tableProperties);
        this.all = ((Boolean)all).booleanValue();

        /* resultColumns cannot be null, so we make a copy of the left RCL
         * for now.  At bind() time, we need to recopy the list because there
         * may have been a "*" in the list.  (We will set the names and
         * column types at that time, as expected.)
         */
        resultColumns = (ResultColumnList)
            getNodeFactory().copyNode(leftResultSet.getResultColumns(),
                                      getParserContext());
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        SetOperatorNode other = (SetOperatorNode)node;
        this.all = other.all;
        this.orderByList = (OrderByList)getNodeFactory().copyNode(other.orderByList,
                                                                  getParserContext());
        this.offset = (ValueNode)getNodeFactory().copyNode(other.offset,
                                                           getParserContext());
        this.fetchFirst = (ValueNode)getNodeFactory().copyNode(other.fetchFirst,
                                                               getParserContext());
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return "all: " + all + "\n" +
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

        if (orderByList != null) {
            printLabel(depth, "orderByList:");
            orderByList.treePrint(depth + 1);
        }
    }

    public boolean isAll() {
        return all;
    }

    /**
     * @return the operator name: "UNION", "INTERSECT", or "EXCEPT"
     */
    abstract String getOperatorName();

}
