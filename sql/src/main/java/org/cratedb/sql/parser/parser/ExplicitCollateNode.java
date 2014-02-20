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

/**
 * This node represents a COLLATE clause attached to an expression.
 *
 */
public class ExplicitCollateNode extends ValueNode
{
    private ValueNode operand;
    private String collation;

    /**
     * Initializer for a ExplicitCollateNode
     *
     * @param operand   The operand
     * @param collation The explicit collation
     */
    public void init(Object operand, Object collation) throws StandardException {
        this.operand = (ValueNode)operand;
        this.collation = (String)collation;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        ExplicitCollateNode other = (ExplicitCollateNode)node;
        this.operand = (ValueNode)getNodeFactory().copyNode(other.operand,
                                                            getParserContext());
        this.collation = other.collation;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return "collation: " + collation + "\n" +
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

        if (operand != null) {
            printLabel(depth, "operand: ");
            operand.treePrint(depth + 1);
        }
    }

    /**
     * Get the operand of this unary operator.
     *
     * @return The operand of this unary operator.
     */
    public ValueNode getOperand() {
        return operand;
    }

    public String getCollation() {
        return collation;
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

        if (operand != null) {
            operand = (ValueNode)operand.accept(v);
        }
    }

    /**
     * @throws StandardException 
     * {@inheritDoc}
     */
    protected boolean isEquivalent(ValueNode o) throws StandardException {
        if (isSameNodeType(o)) {
            // the first condition in the || covers the case when 
            // both operands are null.
            ExplicitCollateNode other = (ExplicitCollateNode)o;
            return (collation.equals(other.collation) && 
                    ((operand == other.operand)|| 
                     ((operand != null) && operand.isEquivalent(other.operand))));
        }
        return false;
    }

}
