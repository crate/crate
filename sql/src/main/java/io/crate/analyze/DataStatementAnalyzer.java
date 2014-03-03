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

package io.crate.analyze;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.ReferenceIdent;
import io.crate.operator.aggregation.impl.CollectSetAggregation;
import io.crate.operator.operator.*;
import io.crate.operator.predicate.NotPredicate;
import io.crate.planner.symbol.*;
import io.crate.planner.symbol.Literal;
import io.crate.sql.tree.*;
import io.crate.sql.tree.BooleanLiteral;
import io.crate.sql.tree.DoubleLiteral;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.StringLiteral;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;

import java.util.*;

abstract class DataStatementAnalyzer<T extends AbstractDataAnalysis> extends AbstractStatementAnalyzer<Symbol, T> {

    private final Map<String, String> swapOperatorTable = ImmutableMap.<String, String>builder()
            .put(GtOperator.NAME, LtOperator.NAME)
            .put(GteOperator.NAME, LteOperator.NAME)
            .put(LtOperator.NAME, GtOperator.NAME)
            .put(LteOperator.NAME, GteOperator.NAME)
            .build();


    @Override
    protected Symbol visitFunctionCall(FunctionCall node, T context) {
        List<Symbol> arguments = new ArrayList<>(node.getArguments().size());
        List<DataType> argumentTypes = new ArrayList<>(node.getArguments().size());
        for (Expression expression : node.getArguments()) {
            ValueSymbol vs = (ValueSymbol) expression.accept(this, context);
            arguments.add(vs);
            argumentTypes.add(vs.valueType());
        }

        FunctionInfo functionInfo = null;
        if (node.isDistinct()) {
            if (argumentTypes.size() > 1) {
                throw new UnsupportedOperationException("Function(DISTINCT x) does not accept more than one argument");
            }
            // define the inner function. use the arguments/argumentTypes from above
            FunctionIdent innerIdent = new FunctionIdent(CollectSetAggregation.NAME, argumentTypes);
            FunctionInfo innerInfo = context.getFunctionInfo(innerIdent);
            Function innerFunction = context.allocateFunction(innerInfo, arguments);

            // define the outer function which contains the inner function as arugment.
            String nodeName = "collection_" + node.getName().toString();
            List<Symbol> outerArguments = Arrays.<Symbol>asList(innerFunction);
            ImmutableList<DataType> outerArgumentTypes = ImmutableList.of(argumentTypes.get(0).setType());

            FunctionIdent ident = new FunctionIdent(nodeName, outerArgumentTypes);
            functionInfo = context.getFunctionInfo(ident);
            arguments = outerArguments;
        } else {
            FunctionIdent ident = new FunctionIdent(node.getName().toString(), argumentTypes);
            functionInfo = context.getFunctionInfo(ident);
        }

        return context.allocateFunction(functionInfo, arguments);
    }

    @Override
    protected Symbol visitInPredicate(InPredicate node, T context) {
        List<Symbol> arguments = new ArrayList<>(2);
        List<DataType> argumentTypes = new ArrayList<>(2);

        Symbol value = process(node.getValue(), context);

        DataType valueDataType = symbolDataTypeVisitor.process(value, null);
        if (valueDataType == DataType.NULL){
            // dynamic or null values cannot be queried, in scalar use-cases (system tables)
            // we currently have no dynamics
            return Null.INSTANCE;
        }

        arguments.add(value);
        SetLiteral inListSymbol = (SetLiteral) process(node.getValueList(), context);

        if (inListSymbol.itemType() != valueDataType) {
            // TODO: implement implicit type casting for set values to valueDataType
            throw new IllegalArgumentException(String.format("invalid type in IN LIST '%s', expected '%s'",
                    inListSymbol.itemType().getName(), valueDataType.getName()));
        }
        arguments.add(inListSymbol);

        argumentTypes.add(valueDataType);
        argumentTypes.add(valueDataType.setType());

        FunctionIdent functionIdent = new FunctionIdent(InOperator.NAME, argumentTypes);
        FunctionInfo functionInfo = context.getFunctionInfo(functionIdent);
        return context.allocateFunction(functionInfo, arguments);
    }

    @Override
    protected Symbol visitIsNotNullPredicate(IsNotNullPredicate node, T context) {
        Symbol argument = process(node.getValue(), context);
        DataType argumentType = symbolDataTypeVisitor.process(argument, null);

        FunctionIdent isNullIdent =
                new FunctionIdent(io.crate.operator.predicate.IsNullPredicate.NAME, ImmutableList.of(argumentType));
        FunctionInfo isNullInfo = context.getFunctionInfo(isNullIdent);

        return context.allocateFunction(
                NotPredicate.INFO,
                ImmutableList.<Symbol>of(context.allocateFunction(isNullInfo, ImmutableList.of(argument))));
    }



    @Override
    protected Symbol visitInListExpression(InListExpression node, T context) {
        Set<Literal> symbols = new HashSet<>();
        DataType dataType = null;
        for (Expression expression : node.getValues()) {
            Symbol s = process(expression, context);
            Preconditions.checkArgument(s.symbolType().isLiteral());
            Literal l = (Literal) s;
            // check dataTypes to be of the same dataType
            if (dataType == null) {
                // first loop run
                dataType = l.valueType();
            } else {
                Preconditions.checkArgument(dataType == l.valueType());
            }
            symbols.add(l);
        }
        return SetLiteral.fromLiterals(dataType, symbols);
    }

    @Override
    protected Symbol visitSubscriptExpression(SubscriptExpression node, T context) {
        SubscriptContext subscriptContext = new SubscriptContext();
        node.accept(visitor, subscriptContext);
        ReferenceIdent ident = new ReferenceIdent(
                context.table().ident(), subscriptContext.column(), subscriptContext.parts());
        return context.allocateReference(ident);
    }

    @Override
    protected Symbol visitLogicalBinaryExpression(LogicalBinaryExpression node, T context) {
        FunctionInfo functionInfo;
        switch (node.getType()) {
            case AND:
                functionInfo = AndOperator.INFO;
                break;
            case OR:
                functionInfo = OrOperator.INFO;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported logical binary expression " + node.getType().name());
        }
        List<Symbol> arguments = new ArrayList<>(2);
        arguments.add(process(node.getLeft(), context));
        arguments.add(process(node.getRight(), context));
        return context.allocateFunction(functionInfo, arguments);
    }

    @Override
    protected Symbol visitNotExpression(NotExpression node, T context) {
        return new Function(NotPredicate.INFO, Arrays.asList(process(node.getValue(), context)));
    }

    @Override
    protected Symbol visitComparisonExpression(ComparisonExpression node, T context) {
        String operatorName = "op_" + node.getType().getValue();

        // resolve arguments
        List<Symbol> arguments = new ArrayList<>(2);
        arguments.add(process(node.getLeft(), context));
        arguments.add(process(node.getRight(), context));

        // resolve argument types
        List<DataType> argumentTypes = new ArrayList<>(arguments.size());
        argumentTypes.add(symbolDataTypeVisitor.process(arguments.get(0), null));
        argumentTypes.add(symbolDataTypeVisitor.process(arguments.get(1), null));


        // swap statements like  eq(2, name) to eq(name, 2)
        if (arguments.get(0).symbolType().isLiteral() &&
                (arguments.get(1).symbolType() == SymbolType.REFERENCE ||
                        arguments.get(1).symbolType() == SymbolType.DYNAMIC_REFERENCE)) {
            if (swapOperatorTable.containsKey(operatorName)) {
                operatorName = swapOperatorTable.get(operatorName);
            }
            Collections.reverse(arguments);
            Collections.reverse(argumentTypes);
        }

        // currently there will be no result for dynamic references, so return here
        if (arguments.get(0).symbolType() == SymbolType.DYNAMIC_REFERENCE){
            return Null.INSTANCE;
        }

        // try implicit type cast (conversion)
        if (argumentTypes.get(0) != argumentTypes.get(1)) {
            Symbol convertedSymbol = ((io.crate.planner.symbol.Literal) arguments.get(1)).convertTo(argumentTypes.get(0));
            arguments.set(1, convertedSymbol);
            argumentTypes.set(1, argumentTypes.get(0));
        }

        // rewrite: exp1 != exp2 to: not(eq(exp1, exp2))
        if (node.getType() == ComparisonExpression.Type.NOT_EQUAL) {
            String eqOperatorName = "op_" + ComparisonExpression.Type.EQUAL.getValue();
            List<DataType> eqArgumentTypes = new ArrayList<>(argumentTypes);
            List<Symbol> eqArguments = new ArrayList<>(arguments);
            FunctionIdent eqFunctionIdent = new FunctionIdent(eqOperatorName, eqArgumentTypes);
            FunctionInfo eqFunctionInfo = context.getFunctionInfo(eqFunctionIdent);
            Function eqFunction = context.allocateFunction(eqFunctionInfo, eqArguments);

            argumentTypes = Arrays.asList(DataType.BOOLEAN);
            arguments = Arrays.<Symbol>asList(eqFunction);
            operatorName = NotPredicate.NAME;
        }

        FunctionIdent functionIdent = new FunctionIdent(operatorName, argumentTypes);
        FunctionInfo functionInfo = context.getFunctionInfo(functionIdent);
        return context.allocateFunction(functionInfo, arguments);
    }

    @Override
    protected Symbol visitLikePredicate(LikePredicate node, T context) {
        if (node.getEscape() != null) {
            throw new UnsupportedOperationException("ESCAPE is not supported yet.");
        }

        // add arguments
        List<Symbol> arguments = new ArrayList<>(2);
        arguments.add(process(node.getValue(), context));
        arguments.add(process(node.getPattern(), context));

        // resolve argument types
        List<DataType> argumentTypes = new ArrayList<>(arguments.size());
        argumentTypes.add(symbolDataTypeVisitor.process(arguments.get(0), null));
        argumentTypes.add(symbolDataTypeVisitor.process(arguments.get(1), null));

        // catch null types, might be null or dynamic reference, which are both not supported
        if (argumentTypes.get(0) == DataType.NULL){
            return Null.INSTANCE;
        }

        FunctionInfo functionInfo = null;
        try {
            // be optimistic. try to look up LikeOperator.
            FunctionIdent functionIdent = new FunctionIdent(LikeOperator.NAME, argumentTypes);
            functionInfo = context.getFunctionInfo(functionIdent);
        } catch (UnsupportedOperationException e1) {
            // check pattern
            if (!(arguments.get(1).symbolType().isLiteral())) {
                throw new UnsupportedOperationException("<expression> LIKE <pattern>: pattern must not be a reference.");
            }
            try {
                tryToCastImplicitly(arguments, argumentTypes, 1, DataType.STRING);
            } catch (UnsupportedOperationException e2) {
                throw new UnsupportedOperationException("<expression> LIKE <pattern>: pattern couldn't be implicitly casted to string. Try to explicitly cast to string.");
            }

            // check expression
            if (argumentTypes.get(0) != DataType.STRING) {
                try {
                    tryToCastImplicitly(arguments, argumentTypes, 0, DataType.STRING);
                } catch (UnsupportedOperationException e3) {
                    throw new UnsupportedOperationException("<expression> LIKE <pattern>: expression couldn't be implicitly casted to string. Try to explicitly cast to string.");
                }
            }

            FunctionIdent functionIdent = new FunctionIdent(LikeOperator.NAME, argumentTypes);
            functionInfo = context.getFunctionInfo(functionIdent);
        }

        return context.allocateFunction(functionInfo, arguments);
    }

    /**
     * Checks if <code>arguments</code> at <code>index</code> is a literal and tries to cast it to given <code>DataType castTo</code>.
     * If successful, the <code>arguments</code> and <code>argumentTypes</code> will be updated.
     * @param arguments
     * @param argumentTypes
     * @param index
     * @param castTo
     * @throws UnsupportedOperationException
     */
    private void tryToCastImplicitly(List<Symbol> arguments, List<DataType> argumentTypes, int index, DataType castTo) throws UnsupportedOperationException {
        if (arguments.get(index).symbolType().isLiteral()) {
            Literal literal = ((Literal) arguments.get(index)).convertTo(castTo); // may throw UnsupportedOperationException.
            arguments.set(index, literal);
            argumentTypes.set(index, literal.valueType());
        } else {
            throw new UnsupportedOperationException("Symbol is not of type Literal");
        }
    }


    @Override
    protected Symbol visitIsNullPredicate(IsNullPredicate node, T context) {

        Symbol value = process(node.getValue(), context);

        // currently there will be no result for dynamic references, so return here
        if (value.symbolType() == SymbolType.DYNAMIC_REFERENCE){
            return Null.INSTANCE;
        }

        FunctionIdent functionIdent =
                new FunctionIdent(io.crate.operator.predicate.IsNullPredicate.NAME,
                        ImmutableList.of(symbolDataTypeVisitor.process(value, null)));
        FunctionInfo functionInfo = context.getFunctionInfo(functionIdent);
        return context.allocateFunction(functionInfo, ImmutableList.of(value));
    }

    @Override
    protected Symbol visitNegativeExpression(NegativeExpression node, T context) {
        // in statements like "where x = -1" the  positive (expression)IntegerLiteral (1)
        // is just wrapped inside a negativeExpression
        // the visitor here swaps it to get -1 in a (symbol)LiteralInteger
        return negativeLiteralVisitor.process(process(node.getValue(), context), null);
    }

    protected void processWhereClause(Expression whereExpression, T context) {
        WhereClause whereClause = context.whereClause(process(whereExpression, context));
        if (whereClause.hasQuery()){
            PrimaryKeyVisitor.Context pkc = primaryKeyVisitor.process(context.table(), whereClause.query());
            if (pkc != null) {
                if (pkc.noMatch) {
                    context.whereClause(WhereClause.NO_MATCH);
                } else {
                    context.primaryKeyLiterals(pkc.keyLiterals());
                    context.version(pkc.version());
                    context.clusteredByLiteral(pkc.clusteredByLiteral());
                }
            }
        }
    }

    @Override
    protected Symbol visitQualifiedNameReference(QualifiedNameReference node, T context) {
        ReferenceIdent ident = context.getReference(node.getName());
        return context.allocateReference(ident);
    }

    @Override
    protected Symbol visitBooleanLiteral(BooleanLiteral node, T context) {
        return new io.crate.planner.symbol.BooleanLiteral(node.getValue());
    }

    @Override
    protected Symbol visitStringLiteral(StringLiteral node, T context) {
        return new io.crate.planner.symbol.StringLiteral(new BytesRef(node.getValue()));
    }

    @Override
    protected Symbol visitDoubleLiteral(DoubleLiteral node, T context) {
        return new io.crate.planner.symbol.DoubleLiteral(node.getValue());
    }

    @Override
    protected Symbol visitLongLiteral(LongLiteral node, T context) {
        return new io.crate.planner.symbol.LongLiteral(node.getValue());
    }

    @Override
    protected Symbol visitNullLiteral(NullLiteral node, T context) {
        return Null.INSTANCE;
    }

    @Override
    public Symbol visitParameterExpression(ParameterExpression node, T context) {
        Object parameter = context.parameterAt(node.index());
        try {
            return io.crate.planner.symbol.Literal.forValue(parameter);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unsupported parameter type " + parameter.getClass());
        }
    }
}
