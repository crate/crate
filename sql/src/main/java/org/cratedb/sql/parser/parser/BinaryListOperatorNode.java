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
 * A BinaryListOperatorNode represents a built-in "binary" operator with a single
 * operand on the left of the operator and a list of operands on the right.
 * This covers operators such as IN and BETWEEN.
 *
 */

public abstract class BinaryListOperatorNode extends ValueNode
{
    protected String methodName;
    /* operator used for error messages */
    protected String operator;

    protected ValueNode leftOperand;
    protected ValueNodeList rightOperandList;

    /**
     * Initializer for a BinaryListOperatorNode
     *
     * @param leftOperand The left operand of the node
     * @param rightOperandList The right operand list of the node
     * @param operator String representation of operator
     */

    public void init(Object leftOperand, Object rightOperandList,
                     Object operator, Object methodName) {
        this.leftOperand = (ValueNode)leftOperand;
        this.rightOperandList = (ValueNodeList)rightOperandList;
        this.operator = (String)operator;
        this.methodName = (String)methodName;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        BinaryListOperatorNode other = (BinaryListOperatorNode)node;
        this.methodName = other.methodName;
        this.operator = other.operator;
        this.leftOperand = (ValueNode)
            getNodeFactory().copyNode(other.leftOperand, getParserContext());
        this.rightOperandList = (ValueNodeList)
            getNodeFactory().copyNode(other.rightOperandList, getParserContext());
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return "operator: " + operator + "\n" +
            "methodName: " + methodName + "\n" +
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

        if (leftOperand != null) {
            printLabel(depth, "leftOperand: ");
            leftOperand.treePrint(depth + 1);
        }

        if (rightOperandList != null) {
            printLabel(depth, "rightOperandList: ");
            rightOperandList.treePrint(depth + 1);
        }
    }

    /**
     * Set the leftOperand to the specified ValueNode
     *
     * @param newLeftOperand The new leftOperand
     */
    public void setLeftOperand(ValueNode newLeftOperand) {
        leftOperand = newLeftOperand;
    }

    /**
     * Get the leftOperand
     *
     * @return The current leftOperand.
     */
    public ValueNode getLeftOperand() {
        return leftOperand;
    }

    /**
     * Set the rightOperandList to the specified ValueNodeList
     *
     * @param newRightOperandList The new rightOperandList
     *
     */
    public void setRightOperandList(ValueNodeList newRightOperandList) {
        rightOperandList = newRightOperandList;
    }

    /**
     * Get the rightOperandList
     *
     * @return The current rightOperandList.
     */
    public ValueNodeList getRightOperandList() {
        return rightOperandList;
    }

    /**
     * Return whether or not this expression tree represents a constant expression.
     *
     * @return Whether or not this expression tree represents a constant expression.
     */
    public boolean isConstantExpression() {
        return (leftOperand.isConstantExpression() &&
                rightOperandList.isConstantExpression());
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

        if (leftOperand != null) {
            leftOperand = (ValueNode)leftOperand.accept(v);
        }

        if (rightOperandList != null) {
            rightOperandList = (ValueNodeList)rightOperandList.accept(v);
        }
    }
                
    /**
     * @inheritDoc
     */
    protected boolean isEquivalent(ValueNode o) throws StandardException {
        if (!isSameNodeType(o)) {
            return false;
        }
        BinaryListOperatorNode other = (BinaryListOperatorNode)o;
        if (!operator.equals(other.operator) || 
            !leftOperand.isEquivalent(other.getLeftOperand())) {
            return false;
        }

        if (!rightOperandList.isEquivalent(other.rightOperandList)) {
            return false;
        }

        return true;
    }
}
