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
 * A TernaryOperatorNode represents a built-in ternary operators.
 * This covers  built-in functions like substr().
 * Java operators are not represented here: the JSQL language allows Java
 * methods to be called from expressions, but not Java operators.
 *
 */

public class TernaryOperatorNode extends ValueNode
{
    public static enum OperatorType {
        LOCATE("LOCATE", "locate",
               ValueClassName.NumberDataValue,
               new String[] { ValueClassName.StringDataValue, 
                              ValueClassName.StringDataValue, 
                              ValueClassName.NumberDataValue }),
        SUBSTRING("substring" , "substring",
                  ValueClassName.ConcatableDataValue,
                  new String[] { ValueClassName.ConcatableDataValue, 
                                 ValueClassName.NumberDataValue, 
                                 ValueClassName.NumberDataValue }),
        LIKE("like", "like",
             ValueClassName.BooleanDataValue,
             new String[] { ValueClassName.DataValueDescriptor, 
                            ValueClassName.DataValueDescriptor, 
                            ValueClassName.DataValueDescriptor }),
        TIMESTAMPADD("TIMESTAMPADD", "timestampAdd",
                     ValueClassName.DateTimeDataValue, 
                     // time.timestampadd(interval, count)
                     new String[] { ValueClassName.DateTimeDataValue, 
                                    "java.lang.Integer", 
                                    ValueClassName.NumberDataValue }),
        TIMESTAMPDIFF("TIMESTAMPDIFF", "timestampDiff",
                      ValueClassName.NumberDataValue,
                      // time2.timestampDiff(interval, time1)
                      new String[] {"java.lang.Integer",
                                     ValueClassName.DateTimeDataValue,
                                     ValueClassName.DateTimeDataValue });

        String operator, methodName;
        String resultType;
        String[] argTypes;
        OperatorType(String operator, String methodName,
                     String resultType, String[] argTypes) {
            this.operator = operator;
            this.methodName = methodName;
            this.resultType = resultType;
            this.argTypes = argTypes;
        }
    }

    protected String operator;
    protected String methodName;
    protected OperatorType operatorType;
    protected ValueNode receiver; 

    protected ValueNode leftOperand;
    protected ValueNode rightOperand;

    protected String resultInterfaceType;
    protected String receiverInterfaceType;
    protected String leftInterfaceType;
    protected String rightInterfaceType;

    // TODO: Could be enum, but note how passed as Integer constant.
    public static final int YEAR_INTERVAL = 0;
    public static final int QUARTER_INTERVAL = 1;
    public static final int MONTH_INTERVAL = 2;
    public static final int WEEK_INTERVAL = 3;
    public static final int DAY_INTERVAL = 4;
    public static final int HOUR_INTERVAL = 5;
    public static final int MINUTE_INTERVAL = 6;
    public static final int SECOND_INTERVAL = 7;
    public static final int FRAC_SECOND_INTERVAL = 8;

    /**
     * Initializer for a TernaryOperatorNode
     *
     * @param receiver The receiver (eg, string being operated on in substr())
     * @param leftOperand The left operand of the node
     * @param rightOperand The right operand of the node
     * @param operatorType The type of the operand
     */

    public void init(Object receiver,
                     Object leftOperand,
                     Object rightOperand,
                     Object operatorType,
                     Object trimType) {
        this.receiver = (ValueNode)receiver;
        this.leftOperand = (ValueNode)leftOperand;
        this.rightOperand = (ValueNode)rightOperand;
        this.operatorType = (OperatorType)operatorType;
        this.operator = this.operatorType.operator;
        this.methodName = this.operatorType.methodName;
        this.resultInterfaceType = this.operatorType.resultType;
        this.receiverInterfaceType = this.operatorType.argTypes[0];
        this.leftInterfaceType = this.operatorType.argTypes[1];
        this.rightInterfaceType = this.operatorType.argTypes[2];
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        TernaryOperatorNode other = (TernaryOperatorNode)node;
        this.operator = other.operator;
        this.methodName = other.methodName;
        this.operatorType = other.operatorType;
        this.receiver = (ValueNode)getNodeFactory().copyNode(other.receiver,
                                                             getParserContext());
        this.leftOperand = (ValueNode)getNodeFactory().copyNode(other.leftOperand,
                                                                getParserContext());
        this.rightOperand = (ValueNode)getNodeFactory().copyNode(other.rightOperand,
                                                                 getParserContext());
        this.resultInterfaceType = other.resultInterfaceType;
        this.receiverInterfaceType = other.receiverInterfaceType;
        this.leftInterfaceType = other.leftInterfaceType;
        this.rightInterfaceType = other.rightInterfaceType;
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
            "resultInterfaceType: " + resultInterfaceType + "\n" + 
            "receiverInterfaceType: " + receiverInterfaceType + "\n" + 
            "leftInterfaceType: " + leftInterfaceType + "\n" + 
            "rightInterfaceType: " + rightInterfaceType + "\n" + 
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

        if (receiver != null) {
            printLabel(depth, "receiver: ");
            receiver.treePrint(depth + 1);
        }

        if (leftOperand != null) {
            printLabel(depth, "leftOperand: ");
            leftOperand.treePrint(depth + 1);
        }

        if (rightOperand != null) {
            printLabel(depth, "rightOperand: ");
            rightOperand.treePrint(depth + 1);
        }
    }

    public String getOperator() {
        return operator;
    }

    public String getMethodName() {
        return methodName;
    }

    public ValueNode getReceiver() {
        return receiver;
    }

    public void setReceiver(ValueNode receiver) {
        this.receiver = receiver;
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
     * Accept the visitor for all visitable children of this node.
     * 
     * @param v the visitor
     *
     * @exception StandardException on error
     */
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        if (receiver != null) {
            receiver = (ValueNode)receiver.accept(v);
        }

        if (leftOperand != null) {
            leftOperand = (ValueNode)leftOperand.accept(v);
        }

        if (rightOperand != null) {
            rightOperand = (ValueNode)rightOperand.accept(v);
        }
    }
                
    protected boolean isEquivalent(ValueNode o) throws StandardException {
        if (isSameNodeType(o)) {
            TernaryOperatorNode other = (TernaryOperatorNode)o;

            /*
             * SUBSTR function can either have 2 or 3 arguments.    In the 
             * 2-args case, rightOperand will be null and thus needs 
             * additional handling in the equivalence check.
             */
            return (other.methodName.equals(methodName)
                    && other.receiver.isEquivalent(receiver)
                    && other.leftOperand.isEquivalent(leftOperand)
                    && ( (rightOperand == null && other.rightOperand == null) || 
                         (other.rightOperand != null && 
                          other.rightOperand.isEquivalent(rightOperand)) ) );
        }
        return false;
    }

}
