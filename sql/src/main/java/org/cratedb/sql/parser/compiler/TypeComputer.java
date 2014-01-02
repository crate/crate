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

package org.cratedb.sql.parser.compiler;

import org.cratedb.sql.parser.IncomparableException;
import org.cratedb.sql.parser.parser.*;

import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.types.CharacterTypeAttributes;
import org.cratedb.sql.parser.types.DataTypeDescriptor;
import org.cratedb.sql.parser.types.TypeId;


/** Calculate types from schema information. */
public class TypeComputer implements Visitor
{
    public TypeComputer() {
    }

    public void compute(StatementNode stmt) throws StandardException {
        stmt.accept(this);
    }
    
    protected ValueNode setType(ValueNode node) throws StandardException {
        switch (node.getNodeType()) {
        case EXPLICIT_COLLATE_NODE:
            return collateNode((ExplicitCollateNode)node);
        default:
            node.setType(computeType(node));
            return node;
        }
    }

    /** Probably need to subclass and handle <code>NodeType.COLUMN_REFERENCE</code>
     * to get type propagation started. */
    protected DataTypeDescriptor computeType(ValueNode node) throws StandardException {
        switch (node.getNodeType()) {
        case RESULT_COLUMN:
            return resultColumn((ResultColumn)node);
        case AND_NODE:
        case OR_NODE:
        case IS_NODE:
            return binaryLogicalOperatorNode((BinaryLogicalOperatorNode)node);
        case NOT_NODE:
            return unaryLogicalOperatorNode((UnaryLogicalOperatorNode)node);
        case BINARY_PLUS_OPERATOR_NODE:
        case BINARY_TIMES_OPERATOR_NODE:
        case BINARY_DIVIDE_OPERATOR_NODE:
        case BINARY_DIV_OPERATOR_NODE:
        case BINARY_MINUS_OPERATOR_NODE:
            return binaryArithmeticOperatorNode((BinaryArithmeticOperatorNode)node);
        case BINARY_EQUALS_OPERATOR_NODE:
        case BINARY_NOT_EQUALS_OPERATOR_NODE:
        case BINARY_GREATER_THAN_OPERATOR_NODE:
        case BINARY_GREATER_EQUALS_OPERATOR_NODE:
        case BINARY_LESS_THAN_OPERATOR_NODE:
        case BINARY_LESS_EQUALS_OPERATOR_NODE:
            return binaryComparisonOperatorNode((BinaryComparisonOperatorNode)node);
        case BETWEEN_OPERATOR_NODE:
            return betweenOperatorNode((BetweenOperatorNode)node);
        case IN_LIST_OPERATOR_NODE:
            return inListOperatorNode((InListOperatorNode)node);
        case SUBQUERY_NODE:
            return subqueryNode((SubqueryNode)node);
        case CONDITIONAL_NODE:
            return conditionalNode((ConditionalNode)node);
        case COALESCE_FUNCTION_NODE:
            return coalesceFunctionNode((CoalesceFunctionNode)node);
        case AGGREGATE_NODE:
        case GROUP_CONCAT_NODE:
            return aggregateNode((AggregateNode)node);
        case CONCATENATION_OPERATOR_NODE:
            return concatenationOperatorNode((ConcatenationOperatorNode)node);
        case IS_NULL_NODE:
        case IS_NOT_NULL_NODE:
            return new DataTypeDescriptor(TypeId.BOOLEAN_ID, false);
        case NEXT_SEQUENCE_NODE:
            return new DataTypeDescriptor(TypeId.BIGINT_ID, false);
        case CURRENT_SEQUENCE_NODE:
            return new DataTypeDescriptor(TypeId.BIGINT_ID, false);
        default:
            // assert false;
            return null;
        }
    }

    /** Nodes whose type is inferred from the context. */
    protected static boolean isParameterOrUntypedNull(ValueNode node) {
        switch (node.getNodeType()) {
        case PARAMETER_NODE:
        case UNTYPED_NULL_CONSTANT_NODE:
            return true;
        default:
            return false;
        }
    }

    protected DataTypeDescriptor resultColumn(ResultColumn node)
            throws StandardException {
        ValueNode expr = node.getExpression();
        if (expr == null)
            return null;
        if (isParameterOrUntypedNull(expr) && (expr.getType() == null)) {
            ColumnReference column = node.getReference();
            if (column != null)
                expr.setType(column.getType());
        }
        return expr.getType();
    }

    protected DataTypeDescriptor unaryLogicalOperatorNode(UnaryLogicalOperatorNode node) 
            throws StandardException {
        ValueNode operand = node.getOperand();
        DataTypeDescriptor type = operand.getType();
        if ((type != null) &&
            !type.getTypeId().isBooleanTypeId()) {
            type = new DataTypeDescriptor(TypeId.BOOLEAN_ID, type.isNullable());
            operand = (ValueNode)node.getNodeFactory()
                .getNode(NodeType.CAST_NODE,
                         operand, type, 
                         node.getParserContext());
            node.setOperand(operand);
        }
        if ((type == null) && isParameterOrUntypedNull(operand)) {
            type = new DataTypeDescriptor(TypeId.BOOLEAN_ID, true);
            operand.setType(type);
        }
        return type;
    }

    protected DataTypeDescriptor binaryLogicalOperatorNode(BinaryLogicalOperatorNode node)
            throws StandardException {
        ValueNode leftOperand = node.getLeftOperand();
        ValueNode rightOperand = node.getRightOperand();
        DataTypeDescriptor leftType = leftOperand.getType();
        DataTypeDescriptor rightType = rightOperand.getType();
        if ((leftType != null) &&
            !leftType.getTypeId().isBooleanTypeId()) {
            leftType = new DataTypeDescriptor(TypeId.BOOLEAN_ID, leftType.isNullable());
            leftOperand = (ValueNode)node.getNodeFactory()
                .getNode(NodeType.CAST_NODE,
                         leftOperand, leftType, 
                         node.getParserContext());
            node.setLeftOperand(leftOperand);
        }
        if ((leftType == null) && isParameterOrUntypedNull(leftOperand)) {
            leftType = new DataTypeDescriptor(TypeId.BOOLEAN_ID, true);
            leftOperand.setType(leftType);
        }
        if ((rightType != null) &&
            !rightType.getTypeId().isBooleanTypeId()) {
            rightType = new DataTypeDescriptor(TypeId.BOOLEAN_ID, rightType.isNullable());
            rightOperand = (ValueNode)node.getNodeFactory()
                .getNode(NodeType.CAST_NODE,
                         rightOperand, rightType, 
                         node.getParserContext());
            node.setRightOperand(rightOperand);
        }
        if ((rightType == null) && isParameterOrUntypedNull(rightOperand)) {
            rightType = new DataTypeDescriptor(TypeId.BOOLEAN_ID, true);
            rightOperand.setType(rightType);
        }
        if (node.getNodeType() == NodeType.IS_NODE)
            return new DataTypeDescriptor(TypeId.BOOLEAN_ID, false);
        if (leftType == null) 
            return rightType;
        else if (rightType == null)
            return leftType;
        else
            return leftType.getNullabilityType(leftType.isNullable() || 
                                               rightType.isNullable());
    }

    protected DataTypeDescriptor binaryArithmeticOperatorNode(BinaryArithmeticOperatorNode node)
            throws StandardException {
        ValueNode leftOperand = node.getLeftOperand();
        ValueNode rightOperand = node.getRightOperand();
        DataTypeDescriptor leftType = leftOperand.getType();
        DataTypeDescriptor rightType = rightOperand.getType();
        if (isParameterOrUntypedNull(leftOperand) && (rightType != null)) {
            leftType = rightType.getNullabilityType(true);
            leftOperand.setType(leftType);
        }
        else if (isParameterOrUntypedNull(rightOperand) && (leftType != null)) {
            rightType = leftType.getNullabilityType(true);
            rightOperand.setType(rightType);
        }
        if ((leftType == null) || (rightType == null))
            return null;
        TypeId leftTypeId = leftType.getTypeId();
        TypeId rightTypeId = rightType.getTypeId();

        /* Do any implicit conversions from (long) (var)char. */
        if (leftTypeId.isStringTypeId() && rightTypeId.isNumericTypeId()) {
            boolean nullableResult;
            nullableResult = leftType.isNullable() || rightType.isNullable();

            /* If other side is decimal/numeric, then we need to diddle
             * with the precision, scale and max width in order to handle
             * computations like:    1.1 + '0.111'
             */
            int precision = rightType.getPrecision();
            int scale = rightType.getScale();
            int maxWidth = rightType.getMaximumWidth();

            if (rightTypeId.isDecimalTypeId()) {
                int charMaxWidth = leftType.getMaximumWidth();
                precision += (2 * charMaxWidth);
                scale += charMaxWidth;                              
                maxWidth = precision + 3;
            }

            leftOperand = (ValueNode)node.getNodeFactory()
                .getNode(NodeType.CAST_NODE,
                         leftOperand, 
                         new DataTypeDescriptor(rightTypeId, precision,
                                                scale, nullableResult, 
                                                maxWidth),
                         node.getParserContext());
            node.setLeftOperand(leftOperand);
        }
        else if (rightTypeId.isStringTypeId() && leftTypeId.isNumericTypeId()) {
            boolean nullableResult;
            nullableResult = leftType.isNullable() || rightType.isNullable();

            /* If other side is decimal/numeric, then we need to diddle
             * with the precision, scale and max width in order to handle
             * computations like:    1.1 + '0.111'
             */
            int precision = leftType.getPrecision();
            int scale = leftType.getScale();
            int maxWidth = leftType.getMaximumWidth();

            if (leftTypeId.isDecimalTypeId()) {
                int charMaxWidth = rightType.getMaximumWidth();
                precision += (2 * charMaxWidth);
                scale += charMaxWidth;                              
                maxWidth = precision + 3;
            }
            
            rightOperand = (ValueNode)node.getNodeFactory()
                .getNode(NodeType.CAST_NODE,
                         rightOperand, 
                         new DataTypeDescriptor(leftTypeId, precision,
                                                scale, nullableResult, 
                                                maxWidth),
                         node.getParserContext());
            node.setRightOperand(rightOperand);
        }

        /*
        ** Set the result type of this operator based on the operands.
        ** By convention, the left operand gets to decide the result type
        ** of a binary operator.
        */
        return getTypeCompiler(leftOperand).
            resolveArithmeticOperation(leftOperand.getType(),
                                       rightOperand.getType(),
                                       node.getOperator());
    }

    protected DataTypeDescriptor binaryComparisonOperatorNode(BinaryComparisonOperatorNode node) 
            throws StandardException {
        ValueNode leftOperand = node.getLeftOperand();
        ValueNode rightOperand = node.getRightOperand();

        // Infer type for parameters from other comparand.
        if (isParameterOrUntypedNull(leftOperand)) {
            DataTypeDescriptor rightType = rightOperand.getType();
            if (rightType != null)
                leftOperand.setType(rightType.getNullabilityType(true));
        }
        else if (isParameterOrUntypedNull(rightOperand)) {
            DataTypeDescriptor leftType = leftOperand.getType();
            if (leftType != null)
                rightOperand.setType(leftType.getNullabilityType(true));
        }
            

        TypeId leftTypeId = leftOperand.getTypeId();
        TypeId rightTypeId = rightOperand.getTypeId();

        if ((leftTypeId == null) || (rightTypeId == null))
            return null;

        /*
         * If we are comparing a non-string with a string type, then we
         * must prevent the non-string value from being used to probe into
         * an index on a string column. This is because the string types
         * are all of low precedence, so the comparison rules of the non-string
         * value are used, so it may not find values in a string index because
         * it will be in the wrong order.
         */
        if (!leftTypeId.isStringTypeId() && rightTypeId.isStringTypeId()) {
            DataTypeDescriptor leftType = leftOperand.getType();
            DataTypeDescriptor rightType = rightOperand.getType();

            rightOperand = (ValueNode)node.getNodeFactory()
                .getNode(NodeType.CAST_NODE,
                         rightOperand, 
                         leftType.getNullabilityType(rightType.isNullable()),
                         node.getParserContext());
            node.setRightOperand(rightOperand);
        }
        else if (!rightTypeId.isStringTypeId() && leftTypeId.isStringTypeId()) {
            DataTypeDescriptor leftType = leftOperand.getType();
            DataTypeDescriptor rightType = rightOperand.getType();

            leftOperand = (ValueNode)node.getNodeFactory()
                .getNode(NodeType.CAST_NODE,
                         leftOperand,
                         rightType.getNullabilityType(leftType.isNullable()),
                         node.getParserContext());
            node.setLeftOperand(leftOperand);
        }

        // Bypass the comparable check if this is a rewrite from the 
        // optimizer.    We will assume Mr. Optimizer knows what he is doing.
        if (!node.isForQueryRewrite()) {
            String operator = node.getOperator();
            boolean forEquals = operator.equals("=") || operator.equals("<>");
            boolean cmp = leftOperand.getType().comparable(rightOperand.getType(),
                                                           forEquals);
            if (!cmp) {
                throw new IncomparableException("Types not comparable: " + leftOperand.getType().getTypeName() +
                                            " and " + rightOperand.getType().getTypeName());
            }
        }
        
        /*
        ** Set the result type of this comparison operator based on the
        ** operands.    The result type is always Boolean - the only question
        ** is whether it is nullable or not.    If either of the operands is
        ** nullable, the result of the comparison must be nullable, too, so
        ** we can represent the unknown truth value.
        */
        boolean nullableResult = leftOperand.getType().isNullable() ||
                                 rightOperand.getType().isNullable();
        return new DataTypeDescriptor(TypeId.BOOLEAN_ID, nullableResult);
    }

    protected DataTypeDescriptor betweenOperatorNode(BetweenOperatorNode node) throws StandardException {
        ValueNode leftOperand = node.getLeftOperand();
        DataTypeDescriptor leftType = leftOperand.getType();
        if (leftType == null)
            return null;

        ValueNodeList rightOperands = node.getRightOperandList();
        ValueNode lowOperand = rightOperands.get(0);
        ValueNode highOperand = rightOperands.get(1);
        if (isParameterOrUntypedNull(lowOperand)) {
            lowOperand.setType(leftType.getNullabilityType(true));
        }
        if (isParameterOrUntypedNull(highOperand)) {
            highOperand.setType(leftType.getNullabilityType(true));
        }

        TypeId leftTypeId = leftOperand.getTypeId();
        DataTypeDescriptor lowType = lowOperand.getType();
        DataTypeDescriptor highType = highOperand.getType();
        if (!leftTypeId.isStringTypeId()) {
            if ((lowType != null) && lowType.getTypeId().isStringTypeId()) {
                lowOperand = (ValueNode)node.getNodeFactory()
                    .getNode(NodeType.CAST_NODE,
                             lowOperand,
                             leftType.getNullabilityType(lowType.isNullable()),
                             node.getParserContext());
                rightOperands.set(0, lowOperand);
            }
            if ((highType != null) && highType.getTypeId().isStringTypeId()) {
                highOperand = (ValueNode)node.getNodeFactory()
                    .getNode(NodeType.CAST_NODE,
                             highOperand,
                             leftType.getNullabilityType(highType.isNullable()),
                             node.getParserContext());
                rightOperands.set(1, highOperand);
            }
        }

        if ((lowType == null) || (highType == null))
            return null;
        boolean nullableResult = leftType.isNullable() ||
                                 lowType.isNullable() ||
                                 highType.isNullable();
        return new DataTypeDescriptor(TypeId.BOOLEAN_ID, nullableResult);
    }

    protected DataTypeDescriptor inListOperatorNode(InListOperatorNode node) throws StandardException {
        RowConstructorNode leftOperand = node.getLeftOperand();
        
        if (leftOperand.getNodeList().size() == 1)
        {
            DataTypeDescriptor leftType = leftOperand.getNodeList().get(0).getType();
            if (leftType == null)
                return null;

            boolean nullableResult = leftType.isNullable();

            for (ValueNode rightOperand : node.getRightOperandList().getNodeList()) {
                DataTypeDescriptor rightType;
                if (isParameterOrUntypedNull(rightOperand)) {
                    rightType = leftType.getNullabilityType(true);
                    rightOperand.setType(rightType);
                }
                else {
                    rightType = rightOperand.getType();
                }
                if ((rightType == null) || rightType.isNullable())
                    nullableResult = true;
            }
            return new DataTypeDescriptor(TypeId.BOOLEAN_ID, nullableResult);
        }
        else
        {
            boolean nullable = isNestedTupleNullable(leftOperand)
                                || isNestedTupleNullable(node.getRightOperandList());
            
            return new DataTypeDescriptor(TypeId.BOOLEAN_ID, nullable);
        }
    }
    
    protected boolean isNestedTupleNullable(RowConstructorNode row)
    {
        boolean ret = false;
        
        for (ValueNode node : row.getNodeList())
        {
            if (ret)
                return ret;
            
            if (node instanceof RowConstructorNode)
                ret |= isNestedTupleNullable((RowConstructorNode)node);
            else if (node.getType() == null)
                ret = true;
            else
            {
                ret |= node.getType().isNullable();
            }
        }
        return ret;
    }

    protected DataTypeDescriptor subqueryNode(SubqueryNode node) throws StandardException {
        if (node.getSubqueryType() == SubqueryNode.SubqueryType.EXPRESSION) {
            DataTypeDescriptor col1Type = node.getResultSet().getResultColumns().get(0).getType();
            if (col1Type == null)
                return null;
            else
                return col1Type.getNullabilityType(true);
        }
        else
            return new DataTypeDescriptor(TypeId.BOOLEAN_ID, true);
    }

    protected DataTypeDescriptor conditionalNode(ConditionalNode node) 
            throws StandardException {
        checkBooleanClause(node.getTestCondition(), "WHEN");
        return dominantType(node.getThenElseList());
    }

    protected DataTypeDescriptor coalesceFunctionNode(CoalesceFunctionNode node)
            throws StandardException {
        return dominantType(node.getArgumentsList());
    }

    protected DataTypeDescriptor aggregateNode(AggregateNode node) 
            throws StandardException {
        if (node.getAggregateName().equals("COUNT") ||
            node.getAggregateName().equals("COUNT(*)"))
            return new DataTypeDescriptor(TypeId.BIGINT_ID, false);

        ValueNode operand = node.getOperand();
        if ((operand == null) ||
            (operand.getType() == null))
            return null;
        if (node.getAggregateName().equals("AVG") &&
            operand.getType().getTypeId().isIntegerTypeId())
            return new DataTypeDescriptor(TypeId.DOUBLE_ID, true);
        return operand.getType().getNullabilityType(true);
    }

    protected DataTypeDescriptor concatenationOperatorNode(ConcatenationOperatorNode node)
            throws StandardException {
        ValueNode leftOperand = node.getLeftOperand();
        ValueNode rightOperand = node.getRightOperand();
        DataTypeDescriptor leftType = leftOperand.getType();
        DataTypeDescriptor rightType = rightOperand.getType();
        if ((leftType != null) &&
            !leftType.getTypeId().isStringTypeId()) {
            leftType = new DataTypeDescriptor(TypeId.VARCHAR_ID,
                                              leftType.isNullable(),
                                              leftType.getMaximumWidth());
            leftOperand = (ValueNode)node.getNodeFactory()
                .getNode(NodeType.CAST_NODE,
                         leftOperand, leftType, 
                         node.getParserContext());
            node.setLeftOperand(leftOperand);
        }
        else if (isParameterOrUntypedNull(leftOperand)) {
            leftType = new DataTypeDescriptor(TypeId.VARCHAR_ID, true);
            leftOperand.setType(leftType);
        }
        if ((rightType != null) &&
            !rightType.getTypeId().isStringTypeId()) {
            rightType = new DataTypeDescriptor(TypeId.VARCHAR_ID,
                                              rightType.isNullable(),
                                              rightType.getMaximumWidth());
            rightOperand = (ValueNode)node.getNodeFactory()
                .getNode(NodeType.CAST_NODE,
                         rightOperand, rightType, 
                         node.getParserContext());
            node.setRightOperand(rightOperand);
        }
        else if (isParameterOrUntypedNull(rightOperand)) {
            rightType = new DataTypeDescriptor(TypeId.VARCHAR_ID, true);
            rightOperand.setType(rightType);
        }
        if ((leftType == null) || (rightType == null))
            return null;
        return new DataTypeDescriptor(TypeId.VARCHAR_ID,
                                      leftType.isNullable() || rightType.isNullable(),
                                      leftType.getMaximumWidth() + rightType.getMaximumWidth(),
                                      CharacterTypeAttributes.mergeCollations(leftType.getCharacterAttributes(), rightType.getCharacterAttributes()));
    }

    protected ValueNode collateNode(ExplicitCollateNode node)
            throws StandardException {
        ValueNode operand = node.getOperand();
        DataTypeDescriptor origType = operand.getType();
        if (origType != null) {
            if (!origType.getTypeId().isStringTypeId())
                throw new StandardException("Collation not allowed for " + origType);
            CharacterTypeAttributes characterAttributes =
                CharacterTypeAttributes.forCollation(origType.getCharacterAttributes(),
                                                     node.getCollation());
            operand.setType(new DataTypeDescriptor(origType, characterAttributes));
        }
        return operand;
    }

    protected DataTypeDescriptor dominantType(ValueNodeList nodeList) 
            throws StandardException {
        DataTypeDescriptor result = null;
        for (ValueNode node : nodeList) {
            if (node.getType() == null) continue;
            if (result == null)
                result = node.getType();
            else
                result = result.getDominantType(node.getType());
        }
        if (result != null) {
            for (int i = 0; i < nodeList.size(); i++) {
                ValueNode node = nodeList.get(i);
                if (isParameterOrUntypedNull(node))
                    node.setType(result.getNullabilityType(true));
                else if (addDominantCast(result, node.getType())) {
                    node = (ValueNode)node.getNodeFactory()
                        .getNode(NodeType.CAST_NODE,
                                 node,
                                 result.getNullabilityType(node.getType().isNullable()),
                                 node.getParserContext());
                    nodeList.set(i, node);
                }
            }
        }
        return result;
    }

    protected boolean addDominantCast(DataTypeDescriptor toType,
                                      DataTypeDescriptor fromType) {
        if (fromType == null) return false;
        if (toType.getTypeId().isStringTypeId())
            return !fromType.getTypeId().isStringTypeId();
        return !fromType.getTypeId().equals(toType.getTypeId());
    }

    protected void selectNode(SelectNode node) throws StandardException {
        // Probably the only possible case syntactically is a
        // ColumnReference to a non-boolean column.
        checkBooleanClause(node.getWhereClause(), "WHERE");
        checkBooleanClause(node.getHavingClause(), "HAVING");

        // Children first wasn't enough to ensure that subqueries were done first.
        if (node.getResultColumns() != null)
            node.getResultColumns().accept(this);
    }

    private void checkBooleanClause(ValueNode clause, String which) 
            throws StandardException {
        if (clause != null) {
            DataTypeDescriptor type = clause.getType();
            if (type == null) {
                assert false : "Type not set yet";
                return;
            }
            if (!type.getTypeId().isBooleanTypeId())
                throw new StandardException("Non-boolean " + which + " clause");
        }
    }

    protected void fromSubquery(FromSubquery node) throws StandardException {
        if (node.getResultColumns() != null) {
            ResultColumnList rcl1 = node.getResultColumns(); 
            ResultColumnList rcl2 = node.getSubquery().getResultColumns();
            int size = rcl1.size();
            for (int i = 0; i < size; i++) {
                rcl1.get(i).setType(rcl2.get(i).getType());
            }
        }
    }

    protected void insertNode(InsertNode node) throws StandardException {
    }

    /* Visitor interface. */

    public Visitable visit(Visitable node) throws StandardException {
        if (node instanceof ValueNode) {
            // Value nodes compute type if necessary.
            ValueNode valueNode = (ValueNode)node;
            if (valueNode.getType() == null) {
                return setType(valueNode);
            }
        }
        else {
            // Some structural nodes require special handling.
            switch (((QueryTreeNode)node).getNodeType()) {
            case SELECT_NODE:
                selectNode((SelectNode)node);
                break;
            case FROM_SUBQUERY:
                fromSubquery((FromSubquery)node);
                break;
            case INSERT_NODE:
                insertNode((InsertNode)node);
                break;
            }
        }
        return node;
    }
    
    public boolean skipChildren(Visitable node) throws StandardException {
        return false;
    }
    public boolean visitChildrenFirst(Visitable node) {
        return true;
    }
    public boolean stopTraversal() {
        return false;
    }

    /**
     * Get the TypeCompiler associated with the given TypeId
     *
     * @param typeId The TypeId to get a TypeCompiler for
     *
     * @return The corresponding TypeCompiler
     *
     */
    protected TypeCompiler getTypeCompiler(TypeId typeId) {
        return TypeCompiler.getTypeCompiler(typeId);
    }

    /**
     * Get the TypeCompiler from this ValueNode, based on its TypeId
     * using getTypeId().
     *
     * @return This ValueNode's TypeCompiler
     *
     */
    protected TypeCompiler getTypeCompiler(ValueNode valueNode) throws StandardException {
        return getTypeCompiler(valueNode.getTypeId());
    }

}
