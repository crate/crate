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
 * A SingleChildResultSetNode represents a result set with a single child.
 *
 */

abstract class SingleChildResultSetNode extends FromTable
{
    /**
     * ResultSetNode under the SingleChildResultSetNode
     */
    ResultSetNode childResult;

    /**
     * Initialilzer for a SingleChildResultSetNode.
     *
     * @param childResult The child ResultSetNode
     * @param tableProperties Properties list associated with the table
     */

    public void init(Object childResult, Object tableProperties) {
        /* correlationName is always null */
        super.init(null, tableProperties);
        this.childResult = (ResultSetNode)childResult;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        SingleChildResultSetNode other = (SingleChildResultSetNode)node;
        this.childResult = (ResultSetNode)getNodeFactory().copyNode(other.childResult,
                                                                    getParserContext());
    }

    /**
     * Return the childResult from this node.
     *
     * @return ResultSetNode The childResult from this node.
     */
    public ResultSetNode getChildResult() {
        return childResult;
    }

    /**
     * Set the childResult for this node.
     *
     * @param childResult The new childResult for this node.
     */
    void setChildResult(ResultSetNode childResult) {
        this.childResult = childResult;
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        super.printSubNodes(depth);

        if (childResult != null) {
            printLabel(depth, "childResult: ");
            childResult.treePrint(depth + 1);
        }
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

        if (childResult != null) {
            childResult = (ResultSetNode)childResult.accept(v);
        }
    }

}
