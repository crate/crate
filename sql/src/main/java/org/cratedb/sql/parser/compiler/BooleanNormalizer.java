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

/* The original from which this derives bore the following: */

/*

   Derby - Class org.apache.derby.impl.sql.compile.AndNode

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.cratedb.sql.parser.compiler;

import org.cratedb.sql.parser.parser.*;

import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.types.DataTypeDescriptor;
import org.cratedb.sql.parser.types.TypeId;

/** Perform normalization such as CNF on boolean expressions. */
public class BooleanNormalizer implements Visitor
{
    public static final int NOT_IN_AND_LIMIT = 100;

    SQLParserContext parserContext;
    NodeFactory nodeFactory;
    public BooleanNormalizer(SQLParserContext parserContext) {
        this.parserContext = parserContext;
        this.nodeFactory = parserContext.getNodeFactory();
    }

    /** Normalize conditions anywhere in this statement. */
    public StatementNode normalize(StatementNode stmt) throws StandardException {
        return (StatementNode)stmt.accept(this);
    }

    /** Normalize WHERE clause in this SELECT node. */
    public void selectNode(SelectNode node) throws StandardException {
        node.setWhereClause(normalizeExpression(node.getWhereClause()));
        node.setHavingClause(normalizeExpression(node.getHavingClause()));
    }

    /** Normalize ON clause in this JOIN node. */
    public void joinNode(JoinNode node) throws StandardException {
        node.setJoinClause(normalizeExpression(node.getJoinClause()));
    }

    /** Normalize WHEN clause in this CASE node. */
    public void conditionalNode(ConditionalNode node) throws StandardException {
        node.setTestCondition(normalizeExpression(node.getTestCondition()));
    }

    /** Normalize a top-level boolean expression. */
    public ValueNode normalizeExpression(ValueNode boolClause) throws StandardException {
        /* For each expression tree:
         *  o Eliminate NOTs (eliminateNots())
         *  o Ensure that there is an AndNode on top of every
         *      top level expression. (putAndsOnTop())
         *  o Finish the job (changeToCNF())
         */
        if (boolClause != null) {
            boolClause = eliminateNots(boolClause, false);
            assert verifyEliminateNots(boolClause);
            boolClause = putAndsOnTop(boolClause);
            assert verifyPutAndsOnTop(boolClause);
            boolClause = changeToCNF(boolClause, true);
            assert verifyChangeToCNF(boolClause, true);
        }
        return boolClause;
    }

    /**
     * Eliminate NotNodes in the current query block.    We traverse the tree, 
     * inverting ANDs and ORs and eliminating NOTs as we go.    We stop at 
     * ComparisonOperators and boolean expressions.  We invert 
     * ComparisonOperators and replace boolean expressions with 
     * boolean expression = false.
     * NOTE: Since we do not recurse under ComparisonOperators, there
     * still could be NotNodes left in the tree.
     *
     * @param node An expression node.
     * @param underNotNode Whether or not we are under a NotNode.
     *                                                                                                      
     * @return The modified expression
     *
     * @exception StandardException                             Thrown on error
     */
    protected ValueNode eliminateNots(ValueNode node, boolean underNotNode)
            throws StandardException {
        switch (node.getNodeType()) {
        case NOT_NODE:
            {
                NotNode notNode = (NotNode)node;
                return eliminateNots(notNode.getOperand(), !underNotNode);
            }
        case AND_NODE:
        case OR_NODE:
            {
                BinaryLogicalOperatorNode bnode = (BinaryLogicalOperatorNode)node;
                ValueNode leftOperand = bnode.getLeftOperand();
                ValueNode rightOperand = bnode.getRightOperand();
                leftOperand = eliminateNots(leftOperand, underNotNode);
                rightOperand = eliminateNots(rightOperand, underNotNode);
                if (underNotNode) {
                    /* Convert AND to OR and vice versa. */
                    BinaryLogicalOperatorNode cnode = (BinaryLogicalOperatorNode)
                        nodeFactory.getNode((node.getNodeType() == NodeType.AND_NODE) ?
                                            NodeType.OR_NODE : NodeType.AND_NODE,
                                            leftOperand, rightOperand,
                                            parserContext);
                    cnode.setType(bnode.getType());
                    return cnode;
                }
                else {
                    bnode.setLeftOperand(leftOperand);
                    bnode.setRightOperand(rightOperand);
                }
            }
            break;
        case CONDITIONAL_NODE:
            {
                ConditionalNode conditionalNode = (ConditionalNode)node;
                ValueNode thenNode = conditionalNode.getThenNode();
                ValueNode elseNode = conditionalNode.getElseNode();
                // TODO: Derby does not do this; is there any benefit?
                thenNode = eliminateNots(thenNode, false);
                elseNode = eliminateNots(elseNode, false);
                if (underNotNode) {
                    ValueNode swap = thenNode;
                    thenNode = elseNode;
                    elseNode = swap;
                }
                conditionalNode.setThenNode(thenNode);
                conditionalNode.setElseNode(elseNode);
            }
            break;
        case IS_NODE:
            {
                IsNode isNode = (IsNode)node;
                ValueNode leftOperand = isNode.getLeftOperand();
                leftOperand = eliminateNots(leftOperand, underNotNode);
                isNode.setLeftOperand(leftOperand);
                if (underNotNode)
                    isNode.toggleNegated();
            }
            break;
        case IS_NULL_NODE:
        case IS_NOT_NULL_NODE:
            if (underNotNode) {
                UnaryOperatorNode unode = (UnaryOperatorNode)node;
                ValueNode operand = unode.getOperand();
                NodeType newNodeType;
                switch (node.getNodeType()) {
                case IS_NULL_NODE:
                    newNodeType = NodeType.IS_NOT_NULL_NODE;
                    break;
                case IS_NOT_NULL_NODE:
                    newNodeType = NodeType.IS_NULL_NODE;
                    break;
                default:
                    assert false;
                    newNodeType = null;
                }
                ValueNode newNode = (ValueNode)nodeFactory.getNode(newNodeType,
                                                                   operand,
                                                                   parserContext);
                newNode.setType(unode.getType());
                return newNode;
            }
            break;
        case BINARY_EQUALS_OPERATOR_NODE:
        case BINARY_GREATER_EQUALS_OPERATOR_NODE:
        case BINARY_GREATER_THAN_OPERATOR_NODE:
        case BINARY_LESS_THAN_OPERATOR_NODE:
        case BINARY_LESS_EQUALS_OPERATOR_NODE:
        case BINARY_NOT_EQUALS_OPERATOR_NODE:
            if (underNotNode) {
                BinaryOperatorNode onode = (BinaryOperatorNode)node;
                ValueNode leftOperand = onode.getLeftOperand();
                ValueNode rightOperand = onode.getRightOperand();
                NodeType newNodeType;
                switch (node.getNodeType()) {
                case BINARY_EQUALS_OPERATOR_NODE:
                    newNodeType = NodeType.BINARY_NOT_EQUALS_OPERATOR_NODE;
                    break;
                case BINARY_GREATER_EQUALS_OPERATOR_NODE:
                    newNodeType = NodeType.BINARY_LESS_THAN_OPERATOR_NODE;
                    break;
                case BINARY_GREATER_THAN_OPERATOR_NODE:
                    newNodeType = NodeType.BINARY_LESS_EQUALS_OPERATOR_NODE;
                    break;
                case BINARY_LESS_THAN_OPERATOR_NODE:
                    newNodeType = NodeType.BINARY_GREATER_EQUALS_OPERATOR_NODE;
                    break;
                case BINARY_LESS_EQUALS_OPERATOR_NODE:
                    newNodeType = NodeType.BINARY_GREATER_THAN_OPERATOR_NODE;
                    break;
                case BINARY_NOT_EQUALS_OPERATOR_NODE:
                    newNodeType = NodeType.BINARY_EQUALS_OPERATOR_NODE;
                    break;
                default:
                    assert false;
                    newNodeType = null;
                }
                ValueNode newNode = (ValueNode)nodeFactory.getNode(newNodeType, 
                                                                                                                     leftOperand, rightOperand,
                                                                                                                     parserContext);
                newNode.setType(onode.getType());
                return newNode;
            }
            break;
        case BETWEEN_OPERATOR_NODE:
            if (underNotNode) {
                BetweenOperatorNode betweenOperatorNode = (BetweenOperatorNode)node;
                ValueNode leftOperand = betweenOperatorNode.getLeftOperand();
                ValueNodeList rightOperandList = betweenOperatorNode.getRightOperandList();
                /* We want to convert the BETWEEN into < OR > as described below. */ 
                /* Convert:
                 *      leftO between rightOList.elementAt(0) and rightOList.elementAt(1)
                 * to:
                 *      leftO < rightOList.elementAt(0) or leftO > rightOList.elementAt(1)
                 * NOTE - We do the conversion here since ORs will eventually be
                 * optimizable and there's no benefit for the optimizer to see NOT BETWEEN
                 */

                /* leftO < rightOList.elementAt(0) */
                BinaryComparisonOperatorNode leftBCO = (BinaryComparisonOperatorNode)
                    nodeFactory.getNode(NodeType.BINARY_LESS_THAN_OPERATOR_NODE,
                                        leftOperand, 
                                        rightOperandList.get(0),
                                        parserContext);

                /* leftO > rightOList.elementAt(1) */
                BinaryComparisonOperatorNode rightBCO = (BinaryComparisonOperatorNode) 
                    nodeFactory.getNode(NodeType.BINARY_GREATER_THAN_OPERATOR_NODE,
                                        leftOperand,
                                        rightOperandList.get(1),
                                        parserContext);

                /* Create and return the OR */
                OrNode newOr = (OrNode)nodeFactory.getNode(NodeType.OR_NODE,
                                                           leftBCO,
                                                           rightBCO,
                                                           parserContext);
                /* Work out types (only nullability). */
                DataTypeDescriptor leftType = leftOperand.getType();
                DataTypeDescriptor right0Type = rightOperandList.get(0).getType();
                DataTypeDescriptor right1Type = rightOperandList.get(1).getType();
                boolean orNullable = false;
                if ((leftType != null) && (right0Type != null)) {
                    boolean nullable = leftType.isNullable() || right0Type.isNullable();
                    DataTypeDescriptor leftBCType = new DataTypeDescriptor(TypeId.BOOLEAN_ID, 
                                                                           nullable);
                    leftBCO.setType(leftBCType);
                    orNullable = nullable;
                }
                if ((leftType != null) && (right1Type != null)) {
                    boolean nullable = leftType.isNullable() || right1Type.isNullable();
                    DataTypeDescriptor rightBCType = new DataTypeDescriptor(TypeId.BOOLEAN_ID, 
                                                                            nullable);
                    rightBCO.setType(rightBCType);
                    orNullable |= nullable;
                }
                if ((leftType != null) && (right0Type != null) && (right1Type != null))
                    newOr.setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID, orNullable));
                return newOr;
            }
            break;
        case IN_LIST_OPERATOR_NODE:
                /* We want to convert the IN List into = OR = ... as * described below. */
                /* Convert:
                 *      leftO IN rightOList.elementAt(0) , rightOList.elementAt(1) ...
                 * to:
                 *      leftO <> rightOList.elementAt(0) AND leftO <> rightOList.elementAt(1) ...
                 * NOTE - We do the conversion here since the single table clauses
                 * can be pushed down and the optimizer may eventually have a filter factor
                 * for <>.
                 */
            if (underNotNode)
                return inWithNestedTuples((InListOperatorNode)node);

            break;
        case SUBQUERY_NODE:
            if (underNotNode) {
                SubqueryNode snode = (SubqueryNode)node;
                SubqueryNode.SubqueryType subqueryType = snode.getSubqueryType();
                switch (subqueryType) {
                case IN:
                    subqueryType = SubqueryNode.SubqueryType.NOT_IN;
                    break;
                case NOT_IN:
                    subqueryType = SubqueryNode.SubqueryType.IN;
                    break;
                case EQ_ANY:
                    subqueryType = SubqueryNode.SubqueryType.NE_ALL;
                    break;
                case EQ_ALL:
                    subqueryType = SubqueryNode.SubqueryType.NE_ANY;
                    break;
                case NE_ANY:
                    subqueryType = SubqueryNode.SubqueryType.EQ_ALL;
                    break;
                case NE_ALL:
                    subqueryType = SubqueryNode.SubqueryType.EQ_ANY;
                    break;
                case GT_ANY:
                    subqueryType = SubqueryNode.SubqueryType.LE_ALL;
                    break;
                case GT_ALL:
                    subqueryType = SubqueryNode.SubqueryType.LE_ANY;
                    break;
                case GE_ANY:
                    subqueryType = SubqueryNode.SubqueryType.LT_ALL;
                    break;
                case GE_ALL:
                    subqueryType = SubqueryNode.SubqueryType.LT_ANY;
                    break;
                case LT_ANY:
                    subqueryType = SubqueryNode.SubqueryType.GE_ALL;
                    break;
                case LT_ALL:
                    subqueryType = SubqueryNode.SubqueryType.GE_ANY;
                    break;
                case LE_ANY:
                    subqueryType = SubqueryNode.SubqueryType.GT_ALL;
                    break;
                case LE_ALL:
                    subqueryType = SubqueryNode.SubqueryType.GT_ANY;
                    break;
                case EXISTS:
                    subqueryType = SubqueryNode.SubqueryType.NOT_EXISTS;
                    break;
                case NOT_EXISTS:
                    subqueryType = SubqueryNode.SubqueryType.EXISTS;
                    break;
                case EXPRESSION:
                    return equalsBooleanConstant(node, Boolean.FALSE);
                default:
                    assert false : "NOT is not supported for this time of subquery";
                    return equalsBooleanConstant(node, Boolean.FALSE);
                }
                snode.setSubqueryType(subqueryType);
            }
            break;
        case BOOLEAN_CONSTANT_NODE:
            if (underNotNode) {
                BooleanConstantNode bnode = (BooleanConstantNode)node;
                bnode.setBooleanValue(!bnode.getBooleanValue());
            }
            break;
        case SQL_BOOLEAN_CONSTANT_NODE:
            if (underNotNode) {
                ConstantNode cnode = (ConstantNode)node;
                cnode.setValue(cnode.getValue() == Boolean.TRUE ? Boolean.FALSE : Boolean.TRUE);
            }
            break;
        case COLUMN_REFERENCE:
            /* X -> (X = TRUE / FALSE) */
            // NOTE: This happened in a different place in the original
            // Derby, but that ended up only doing those along the
            // right-hand branch, which does not seem consistent.
            return equalsBooleanConstant(castToBoolean(node), 
                                         underNotNode ? Boolean.FALSE : Boolean.TRUE);
        default:
            if (underNotNode) {
                return equalsBooleanConstant(castToBoolean(node), Boolean.FALSE);
            }
            else {
                return castToBoolean(node);
            }
        }
        return node;
    }

    protected ValueNode getNotEqual(ValueNode left, ValueNode right) throws StandardException
    {
        if (left instanceof RowConstructorNode)
        {
            if (right instanceof RowConstructorNode)
            {
                ValueNodeList leftList = ((RowConstructorNode)left).getNodeList();
                ValueNodeList rightList = ((RowConstructorNode)right).getNodeList();
                
                if (leftList.size() != rightList.size())
                    throw new IllegalArgumentException("Mismatched column count in IN's operand, left: " 
                                                       + leftList.size() + ", right: " + rightList.size());

                ValueNode result = null;
                for (int n = 0; n < leftList.size(); ++n)
                {
                    ValueNode equalNode = getNotEqual(leftList.get(n), rightList.get(n));
                    
                    if (result == null)
                        result = equalNode;
                    else
                    {
                        OrNode orNode = (OrNode)nodeFactory.getNode(NodeType.OR_NODE,
                                                                       result, equalNode,
                                                                       parserContext);
                        result = orNode;
                    }
                }
                return result;
            }
            else
                throw new IllegalArgumentException("Mismatched column count in IN's operand");
        }
        else
        {
            if (right instanceof RowConstructorNode)
                throw new IllegalArgumentException("Mismatched column count in IN's operands");
           
            return (ValueNode) nodeFactory.getNode(NodeType.BINARY_NOT_EQUALS_OPERATOR_NODE,
                                                   left, right,
                                                   parserContext);
        }
    }

    protected ValueNode inWithNestedTuples(InListOperatorNode node) throws StandardException
    {
        RowConstructorNode rightList = node.getRightOperandList();
        if (rightList.getNodeList().size() > NOT_IN_AND_LIMIT) {
            node.setNegated(true);
            return node;
        }
        RowConstructorNode leftList = node.getLeftOperand();
        ValueNode result = null;
        
        boolean nested = leftList.getDepth() >  0;
        ValueNode left = leftList.getNodeList().get(0);
        for (ValueNode rightNode : rightList.getNodeList())
        {
            ValueNode equalNode = getNotEqual(nested
                                                    ? leftList
                                                    : left
                                              , rightNode);
            
            if (result == null)
                result = equalNode;
            else
            {
                AndNode andNode = (AndNode)nodeFactory.getNode(NodeType.AND_NODE,
                                                            equalNode, result,
                                                            parserContext);
                result = andNode;
            }
        }

        return result;                
    }
    
    protected ValueNode castToBoolean(ValueNode node) throws StandardException {
        if ((node.getType() == null) ||
            (node.getType().getTypeId().isBooleanTypeId()))
            return node;
        return (ValueNode)
            nodeFactory.getNode(NodeType.CAST_NODE,
                                node,
                                new DataTypeDescriptor(TypeId.BOOLEAN_ID, 
                                                       node.getType().isNullable()),
                                parserContext);
    }

    protected ValueNode equalsBooleanConstant(ValueNode node, Boolean constant) 
            throws StandardException {
        BooleanConstantNode trueNode = (BooleanConstantNode)
            nodeFactory.getNode(NodeType.BOOLEAN_CONSTANT_NODE,
                                constant,
                                parserContext);
        BinaryComparisonOperatorNode equalsNode = (BinaryComparisonOperatorNode)
            nodeFactory.getNode(NodeType.BINARY_EQUALS_OPERATOR_NODE,
                                node, trueNode,
                                parserContext);
        if (node.getType() != null) {
            boolean nullableResult = node.getType().isNullable();
            equalsNode.setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID,
                                                      nullableResult));
        }
        return equalsNode;
    }

    /**
     * Verify that eliminateNots() did its job correctly.    Verify that
     * there are no NotNodes above the top level comparison operators
     * and boolean expressions.
     *
     * @return Boolean which reflects validity of the tree.
     */
    protected boolean verifyEliminateNots(ValueNode node) {
        switch (node.getNodeType()) {
        case NOT_NODE:
            return false;
        case AND_NODE:
        case OR_NODE:
            {
                BinaryLogicalOperatorNode bnode = (BinaryLogicalOperatorNode)node;
                return verifyEliminateNots(bnode.getLeftOperand()) &&
                       verifyEliminateNots(bnode.getRightOperand());
            }
        }
        return true;
    }

    /**
     * Do the 1st step in putting an expression into conjunctive normal
     * form.    This step ensures that the top level of the expression is
     * a chain of AndNodes terminated by a true BooleanConstantNode.
     *
     * @param node An expression node.
     *
     * @return The modified expression
     *
     * @exception StandardException Thrown on error
     */
    protected AndNode putAndsOnTop(ValueNode node) throws StandardException {
        switch (node.getNodeType()) {
        case AND_NODE:
            {
                AndNode andNode = (AndNode)node;
                andNode.setRightOperand(putAndsOnTop(andNode.getRightOperand()));
                return andNode;
            }
        default:
            {
                /* expr -> expr AND TRUE */
                BooleanConstantNode trueNode = (BooleanConstantNode)
                    nodeFactory.getNode(NodeType.BOOLEAN_CONSTANT_NODE,
                                        Boolean.TRUE,
                                        parserContext);
                AndNode andNode = (AndNode)nodeFactory.getNode(NodeType.AND_NODE,
                                                               node, trueNode,
                                                               parserContext);
                if (node.getType() != null) {
                    boolean nullableResult = node.getType().isNullable();
                    andNode.setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID,
                                                           nullableResult));
                }
                return andNode;
            }
        }
    }

    /**
     * Verify that putAndsOnTop() did its job correctly.    Verify that the top level 
     * of the expression is a chain of AndNodes terminated by a true BooleanConstantNode.
     *
     * @param node An expression node.
     *
     * @return Boolean which reflects validity of the tree.
     */
    protected boolean verifyPutAndsOnTop(ValueNode node) {
        while (true) {
            if (!(node instanceof AndNode)) 
                return false;
            node = ((AndNode)node).getRightOperand();
            if (node.isBooleanTrue())
                return true;
            // else another AND.
        }
    }

    /**
     * Finish putting an expression into conjunctive normal
     * form.    An expression tree in conjunctive normal form meets
     * the following criteria:
     *      o    If the expression tree is not null,
     *           the top level will be a chain of AndNodes terminating
     *           in a true BooleanConstantNode.
     *      o    The left child of an AndNode will never be an AndNode.
     *      o    Any right-linked chain that includes an AndNode will
     *           be entirely composed of AndNodes terminated by a true BooleanConstantNode.
     *      o    The left child of an OrNode will never be an OrNode.
     *      o    Any right-linked chain that includes an OrNode will
     *           be entirely composed of OrNodes terminated by a false BooleanConstantNode.
     *      o    ValueNodes other than AndNodes and OrNodes are considered
     *           leaf nodes for purposes of expression normalization.
     *           In other words, we won't do any normalization under
     *           those nodes.
     *
     * In addition, we track whether or not we are under a top level AndNode.    
     * SubqueryNodes need to know this for subquery flattening.
     *
     * @param node An expression node.
     * @param underTopAndNode Whether or not we are under a top level AndNode.
     *
     * @return The modified expression
     *
     * @exception StandardException Thrown on error
     */
    protected ValueNode changeToCNF(ValueNode node, boolean underTopAndNode)
            throws StandardException {
        switch (node.getNodeType()) {
        case AND_NODE:
            {
                AndNode andNode = (AndNode)node;
                ValueNode leftOperand = andNode.getLeftOperand();
                ValueNode rightOperand = andNode.getRightOperand();

                /* Top chain will be a chain of Ands terminated by a non-AndNode.
                 * (putAndsOnTop() has taken care of this. If the last node in
                 * the chain is not a true BooleanConstantNode then we need to do the
                 * transformation to make it so.
                 */

                /* Add the true BooleanConstantNode if not there yet */
                if (!(rightOperand instanceof AndNode) &&
                        !(rightOperand.isBooleanTrue())) {
                    BooleanConstantNode trueNode = (BooleanConstantNode) 
                        nodeFactory.getNode(NodeType.BOOLEAN_CONSTANT_NODE,
                                            Boolean.TRUE,
                                            parserContext);
                    AndNode newRight = (AndNode)nodeFactory.getNode(NodeType.AND_NODE,
                                                                    rightOperand, trueNode,
                                                                    parserContext);
                    newRight.setType(rightOperand.getType());
                    rightOperand = newRight;
                }

                /* If leftOperand is an AndNode, then we modify the tree from:
                 *
                 *                              And1
                 *                           /      \
                 *                      And2        Nodex
                 *                   /      \                ...
                 *              left2        right2
                 *
                 *      to:
                 *
                 *                                              And1
                 *                                           /      \
                 *       changeToCNF(left2)          And2
                 *                                                      /        \
                 *               changeToCNF(right2)            changeToCNF(Nodex)
                 *
                 *  NOTE: We could easily switch places between changeToCNF(left2) and 
                 *  changeToCNF(right2).
                 */

                /* Pull up the AndNode chain to our left */
                while (leftOperand instanceof AndNode) {
                    AndNode oldLeft = (AndNode)leftOperand;
                    ValueNode oldRight = rightOperand;
                    ValueNode newLeft = oldLeft.getLeftOperand();
                    AndNode newRight = oldLeft;
                    
                    /* We then twiddle the tree to match the above diagram */
                    leftOperand = newLeft;
                    rightOperand = newRight;
                    newRight.setLeftOperand(oldLeft.getRightOperand());
                    newRight.setRightOperand(oldRight);
                }
                
                /* We then twiddle the tree to match the above diagram */
                leftOperand = changeToCNF(leftOperand, underTopAndNode);
                rightOperand = changeToCNF(rightOperand, underTopAndNode);
                
                andNode.setLeftOperand(leftOperand);
                andNode.setRightOperand(rightOperand);
            }
            break;
        case OR_NODE:
            {
                OrNode orNode = (OrNode)node;
                ValueNode leftOperand = orNode.getLeftOperand();
                ValueNode rightOperand = orNode.getRightOperand();

                /* If rightOperand is an AndNode, then we must generate an 
                 * OrNode above it.
                 */
                if (rightOperand instanceof AndNode) {
                    BooleanConstantNode falseNode = (BooleanConstantNode) 
                        nodeFactory.getNode(NodeType.BOOLEAN_CONSTANT_NODE,
                                            Boolean.FALSE,
                                            parserContext);
                    OrNode newRight = (OrNode)nodeFactory.getNode(NodeType.OR_NODE,
                                                                  rightOperand, falseNode,
                                                                  parserContext);
                    newRight.setType(rightOperand.getType());
                    rightOperand = newRight;
                    orNode.setRightOperand(rightOperand);
                }
                
                /* We need to ensure that the right chain is terminated by
                 * a false BooleanConstantNode.
                 */
                while (rightOperand instanceof OrNode) {
                    orNode = (OrNode)orNode.getRightOperand();
                    rightOperand = orNode.getRightOperand();
                }

                /* Add the false BooleanConstantNode if not there yet */
                if (!rightOperand.isBooleanFalse()) {
                    BooleanConstantNode falseNode = (BooleanConstantNode) 
                        nodeFactory.getNode(NodeType.BOOLEAN_CONSTANT_NODE,
                                            Boolean.FALSE,
                                            parserContext);
                    OrNode newRight = (OrNode)nodeFactory.getNode(NodeType.OR_NODE,
                                                                  rightOperand, 
                                                                  falseNode,
                                                                  parserContext);
                    newRight.setType(rightOperand.getType());
                    orNode.setRightOperand(newRight);
                }

                orNode = (OrNode)node;
                rightOperand = orNode.getRightOperand();

                /* If leftOperand is an OrNode, then we modify the tree from:
                 *
                 *                              Or1 
                 *                           /      \
                 *                      Or2              Nodex
                 *                   /      \                ...
                 *              left2        right2
                 *
                 *      to:
                 *
                 *                                              Or1 
                 *                                           /      \
                 *       changeToCNF(left2)          Or2
                 *                                                      /        \
                 *               changeToCNF(right2)            changeToCNF(Nodex)
                 *
                 *  NOTE: We could easily switch places between changeToCNF(left2) and 
                 *  changeToCNF(right2).
                 */

                while (leftOperand instanceof OrNode) {
                    OrNode oldLeft = (OrNode)leftOperand;
                    ValueNode oldRight = rightOperand;
                    ValueNode newLeft = oldLeft.getLeftOperand();
                    OrNode newRight = oldLeft;

                    /* We then twiddle the tree to match the above diagram */
                    leftOperand = newLeft;
                    rightOperand = newRight;
                    newRight.setLeftOperand(oldLeft.getRightOperand());
                    newRight.setRightOperand(oldRight);
                }

                /* Finally, we continue to normalize the left and right subtrees. */
                leftOperand = changeToCNF(leftOperand, false);
                rightOperand = changeToCNF(rightOperand, false);
                
                orNode.setLeftOperand(leftOperand);
                orNode.setRightOperand(rightOperand);
            }
            break;

            // TODO: subquery node to pick up underTopAndNode for flattening.
            // BinaryComparisonOperatorNode for that case.

        }
        return node;
    }

    /**
     * Verify that changeToCNF() did its job correctly.  Verify that:
     *      o    AndNode    - rightOperand is not instanceof OrNode
     *                                  leftOperand is not instanceof AndNode
     *      o    OrNode      - rightOperand is not instanceof AndNode
     *                                  leftOperand is not instanceof OrNode
     *
     * @param node An expression node.
     *
     * @return Boolean which reflects validity of the tree.
     */
    protected boolean verifyChangeToCNF(ValueNode node, boolean top) {
        if (node instanceof AndNode) {
            AndNode andNode = (AndNode)node;
            ValueNode leftOperand = andNode.getLeftOperand();
            ValueNode rightOperand = andNode.getRightOperand();
            boolean isValid = ((rightOperand instanceof AndNode) ||
                               rightOperand.isBooleanTrue());
            if (rightOperand instanceof AndNode) {
                isValid = verifyChangeToCNF(rightOperand, false);
            }
            if (leftOperand instanceof AndNode) {
                isValid = false;
            }
            else {
                isValid = isValid && verifyChangeToCNF(leftOperand, false);
            }
            return isValid;
        }
        if (top) 
            return false;
        if (node instanceof OrNode) {
            OrNode orNode = (OrNode)node;
            ValueNode leftOperand = orNode.getLeftOperand();
            ValueNode rightOperand = orNode.getRightOperand();
            boolean isValid = ((rightOperand instanceof OrNode) ||
                               rightOperand.isBooleanFalse());
            if (rightOperand instanceof OrNode) {
                isValid = verifyChangeToCNF(rightOperand, false);
            }
            if (leftOperand instanceof OrNode) {
                isValid = false;
            }
            else {
                isValid = verifyChangeToCNF(leftOperand, false);
            }
            return isValid;
        }
        return true;
    }

    /* Visitor interface */

    public Visitable visit(Visitable node) throws StandardException {
        switch (((QueryTreeNode)node).getNodeType()) {
        case SELECT_NODE:
            selectNode((SelectNode)node);
            break;
        case JOIN_NODE:
        case HALF_OUTER_JOIN_NODE:
            joinNode((JoinNode)node);
            break;
        case CONDITIONAL_NODE:
            conditionalNode((ConditionalNode)node);
            break;
        }
        return node;
    }

    public boolean visitChildrenFirst(Visitable node) {
        return true;
    }
    public boolean stopTraversal() {
        return false;
    }
    public boolean skipChildren(Visitable node) throws StandardException {
        return false;
    }

}
