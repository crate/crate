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
import org.cratedb.sql.parser.types.ValueClassName;

/**
 * A BinaryOperatorNode represents a built-in binary operator as defined by
 * the ANSI/ISO SQL standard.    This covers operators like +, -, *, /, =, <, etc.
 * Java operators are not represented here: the JSQL language allows Java
 * methods to be called from expressions, but not Java operators.
 *
 */

public class BinaryOperatorNode extends ValueNode
{
    protected String operator;
    protected String methodName;

    public static enum OperatorType {
        PLUS, MINUS, TIMES, DIVIDE, CONCATENATE, MOD, DIV,
        EQ, NE, GT, GE, LT, LE, AND, OR, LIKE, LTRIM, TRIM, RTRIM,
        BITAND, BITOR, BITXOR, LEFT_SHIFT, RIGHT_SHIFT,
        LEFT, RIGHT
    }

    protected ValueNode leftOperand;
    protected ValueNode rightOperand;

    protected String leftInterfaceType;
    protected String rightInterfaceType;
    protected String resultInterfaceType;

    /**
     * Initializer for a BinaryOperatorNode
     *
     * @param leftOperand The left operand of the node
     * @param rightOperand The right operand of the node
     * @param operator The name of the operator
     * @param methodName The name of the method to call for this operator
     * @param leftInterfaceType The name of the interface for the left operand
     * @param rightInterfaceType The name of the interface for the right operand
     */

    public void init(Object leftOperand, Object rightOperand,
                     Object operator, Object methodName,
                     Object leftInterfaceType, Object rightInterfaceType) {
        this.leftOperand = (ValueNode)leftOperand;
        this.rightOperand = (ValueNode)rightOperand;
        this.operator = (String)operator;
        this.methodName = (String)methodName;
        this.leftInterfaceType = (String)leftInterfaceType;
        this.rightInterfaceType = (String)rightInterfaceType;
    }

    public void init(Object leftOperand, Object rightOperand, 
                     Object leftInterfaceType, Object rightInterfaceType) {
        this.leftOperand = (ValueNode)leftOperand;
        this.rightOperand = (ValueNode)rightOperand;
        this.leftInterfaceType = (String)leftInterfaceType;
        this.rightInterfaceType = (String)rightInterfaceType;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        BinaryOperatorNode other = (BinaryOperatorNode)node;
        this.operator = other.operator;
        this.methodName = other.methodName;
        this.leftOperand = (ValueNode)
            getNodeFactory().copyNode(other.leftOperand, getParserContext());
        this.rightOperand = (ValueNode)
            getNodeFactory().copyNode(other.rightOperand, getParserContext());
        this.leftInterfaceType = other.leftInterfaceType;
        this.rightInterfaceType = other.rightInterfaceType;
        this.resultInterfaceType = other.resultInterfaceType;
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
     * Set the operator.
     *
     * @param operator The operator.
     */
    void setOperator(String operator) {
        this.operator = operator;
    }

    public String getOperator() {
        return operator;
    }

    /**
     * Set the methodName.
     *
     * @param methodName The methodName.
     */
    void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public String getMethodName() {
        return methodName;
    }

    /**
     * Set the interface type for the left and right arguments.
     * Used when we don't know the interface type until
     * later in binding.
     */
    public void setLeftRightInterfaceType(String iType) {
        leftInterfaceType = iType;
        rightInterfaceType = iType;
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

        if (rightOperand != null) {
            printLabel(depth, "rightOperand: ");
            rightOperand.treePrint(depth + 1);
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
     * Set the rightOperand to the specified ValueNode
     *
     * @param newRightOperand The new rightOperand
     */
    public void setRightOperand(ValueNode newRightOperand) {
        rightOperand = newRightOperand;
    }

    /**
     * Get the rightOperand
     *
     * @return The current rightOperand.
     */
    public ValueNode getRightOperand() {
        return rightOperand;
    }

    /**
     * Return whether or not this expression tree represents a constant expression.
     *
     * @return Whether or not this expression tree represents a constant expression.
     */
    public boolean isConstantExpression() {
        return (leftOperand.isConstantExpression() &&
                rightOperand.isConstantExpression());
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

        if (rightOperand != null) {
            rightOperand = (ValueNode)rightOperand.accept(v);
        }
    }

    /**
     * @inheritDoc
     */
    protected boolean isEquivalent(ValueNode o) throws StandardException {
        if (!isSameNodeType(o)) {
            return false;
        }
        BinaryOperatorNode other = (BinaryOperatorNode)o;
        return methodName.equals(other.methodName) && 
            leftOperand.isEquivalent(other.leftOperand) && 
            rightOperand.isEquivalent(other.rightOperand);
    }

}
