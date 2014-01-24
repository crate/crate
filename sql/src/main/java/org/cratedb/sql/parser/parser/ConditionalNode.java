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
 * A ConditionalNode represents an if/then/else operator with a single
 * boolean expression on the "left" of the operator and a list of expressions on 
 * the "right". This is used to represent the java conditional (aka immediate if).
 *
 */

public class ConditionalNode extends ValueNode
{
    private ValueNode testCondition;
    private ValueNodeList thenElseList;

    // True means we are here for NULLIF(V1,V2), false means we are here for following
    // CASE WHEN BooleanExpression THEN thenExpression ELSE elseExpression END
    private boolean thisIsNullIfNode;

    /**
     * Initializer for a ConditionalNode
     *
     * @param testCondition The boolean test condition
     * @param thenElseList ValueNodeList with then and else expressions
     */

    public void init(Object testCondition, Object thenElseList, Object thisIsNullIfNode) {
        this.testCondition = (ValueNode)testCondition;
        this.thenElseList = (ValueNodeList)thenElseList;
        this.thisIsNullIfNode = ((Boolean)thisIsNullIfNode).booleanValue();
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        ConditionalNode other = (ConditionalNode)node;
        this.testCondition = (ValueNode)
            getNodeFactory().copyNode(other.testCondition, getParserContext());
        this.thenElseList = (ValueNodeList)
            getNodeFactory().copyNode(other.thenElseList, getParserContext());
        this.thisIsNullIfNode = other.thisIsNullIfNode;
    }

    public ValueNode getTestCondition() {
        return testCondition;
    }

    public void setTestCondition(ValueNode testCondition) {
        this.testCondition = testCondition;
    }

    public ValueNodeList getThenElseList() {
        return thenElseList;
    }

    public ValueNode getThenNode() {
        return thenElseList.get(0);
    }

    public void setThenNode(ValueNode thenNode) {
        thenElseList.set(0, thenNode);
    }

    public ValueNode getElseNode() {
        return thenElseList.get(1);
    }

    public void setElseNode(ValueNode elseNode) {
        thenElseList.set(1, elseNode);
    }

    public boolean isNullIfNode() {
        return thisIsNullIfNode;
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        super.printSubNodes(depth);

        if (testCondition != null) {
            printLabel(depth, "testCondition: ");
            testCondition.treePrint(depth + 1);
        }

        if (thenElseList != null) {
            printLabel(depth, "thenElseList: ");
            thenElseList.treePrint(depth + 1);
        }
    }

    /**
     * Return whether or not this expression tree represents a constant expression.
     *
     * @return Whether or not this expression tree represents a constant expression.
     */
    public boolean isConstantExpression() {
        return (testCondition.isConstantExpression() &&
                thenElseList.isConstantExpression());
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

        if (testCondition != null) {
            testCondition = (ValueNode)testCondition.accept(v);
        }

        if (thenElseList != null) {
            thenElseList = (ValueNodeList)thenElseList.accept(v);
        }
    }
                
    /**
     * {@inheritDoc}
     */
    protected boolean isEquivalent(ValueNode o) throws StandardException {
        if (isSameNodeType(o)) {
            ConditionalNode other = (ConditionalNode)o;
            return testCondition.isEquivalent(other.testCondition) &&
                thenElseList.isEquivalent(other.thenElseList);
        }
        return false;
    }

}
