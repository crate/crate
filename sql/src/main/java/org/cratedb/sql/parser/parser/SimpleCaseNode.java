/**
 * Copyright 2011-2013 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;

/**
 * A SimpleCaseNode represents the CASE <expr> THEN ... form.
 */

public class SimpleCaseNode extends ValueNode
{
    private ValueNode operand;
    private ValueNodeList caseOperands, resultValues;
    private ValueNode elseValue;

    /**
     * Initializer for a SimpleCaseNode
     *
     * @param operand The expression being compared
     */

    public void init(Object operand) throws StandardException {
        this.operand = (ValueNode)operand;
        this.caseOperands = (ValueNodeList)getNodeFactory().getNode(NodeType.VALUE_NODE_LIST,
                                                                    getParserContext());
        this.resultValues = (ValueNodeList)getNodeFactory().getNode(NodeType.VALUE_NODE_LIST,
                                                                    getParserContext());
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        SimpleCaseNode other = (SimpleCaseNode)node;
        this.operand = (ValueNode)
            getNodeFactory().copyNode(other.operand, getParserContext());
        this.caseOperands = (ValueNodeList)
            getNodeFactory().copyNode(other.caseOperands, getParserContext());
        this.resultValues = (ValueNodeList)
            getNodeFactory().copyNode(other.resultValues, getParserContext());
        if (other.elseValue == null)
            this.elseValue = null;
        else
            this.elseValue = (ValueNode)
                getNodeFactory().copyNode(other.elseValue, getParserContext());
    }

    public ValueNode getOperand() {
        return operand;
    }

    public ValueNodeList getCaseOperands() {
        return caseOperands;
    }

    public ValueNodeList getResultValues() {
        return resultValues;
    }

    public ValueNode getElseValue() {
        return elseValue;
    }

    public void setElseValue(ValueNode elseValue) {
        this.elseValue = elseValue;
    }

    /** The number of <code>WHEN</code> cases. */
    public int getNumberOfCases() {
        return caseOperands.size();
    }

    /** The <code>WHEN</code> part. */
    public ValueNode getCaseOperand(int index) {
        return caseOperands.get(index);
    }

    /** The <code>THEN</code> part. */
    public ValueNode getResultValue(int index) {
        return resultValues.get(index);
    }

    public void addCase(ValueNode operand, ValueNode result) {
        caseOperands.add(operand);
        resultValues.add(result);
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        super.printSubNodes(depth);

        printLabel(depth, "operand: ");
        operand.treePrint(depth + 1);

        for (int i = 0; i < getNumberOfCases(); i++) {
            printLabel(depth, "when: ");
            getCaseOperand(i).treePrint(depth + 1);
            printLabel(depth, "then: ");
            getResultValue(i).treePrint(depth + 1);
        }

        if (elseValue != null) {
            printLabel(depth, "else: ");
            elseValue.treePrint(depth + 1);
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

        operand = (ValueNode)operand.accept(v);
        caseOperands = (ValueNodeList)caseOperands.accept(v);
        resultValues = (ValueNodeList)resultValues.accept(v);
        if (elseValue != null)
            elseValue = (ValueNode)elseValue.accept(v);
    }
                
    /**
     * {@inheritDoc}
     */
    protected boolean isEquivalent(ValueNode o) throws StandardException {
        if (isSameNodeType(o)) {
            SimpleCaseNode other = (SimpleCaseNode)o;
            return operand.isEquivalent(other.operand) &&
                caseOperands.isEquivalent(other.caseOperands) &&
                resultValues.isEquivalent(other.resultValues) &&
                ((elseValue == null) ? (other.elseValue == null) :
                 elseValue.isEquivalent(other.elseValue));
        }
        return false;
    }

}
