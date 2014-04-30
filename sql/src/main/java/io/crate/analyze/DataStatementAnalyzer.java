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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.crate.DataType;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.ReferenceIdent;
import io.crate.operation.aggregation.impl.CollectSetAggregation;
import io.crate.operation.operator.AndOperator;
import io.crate.operation.operator.InOperator;
import io.crate.operation.operator.LikeOperator;
import io.crate.operation.operator.OrOperator;
import io.crate.operation.operator.any.AnyOperator;
import io.crate.operation.predicate.NotPredicate;
import io.crate.planner.symbol.*;
import io.crate.planner.symbol.Literal;
import io.crate.sql.tree.*;
import io.crate.sql.tree.BooleanLiteral;
import io.crate.sql.tree.DoubleLiteral;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.StringLiteral;
import org.apache.lucene.util.BytesRef;

import java.util.*;


abstract class DataStatementAnalyzer<T extends AbstractDataAnalysis> extends AbstractStatementAnalyzer<Symbol, T> {

    private final Map<ComparisonExpression.Type, ComparisonExpression.Type> swapOperatorTable = ImmutableMap.<ComparisonExpression.Type, ComparisonExpression.Type>builder()
            .put(ComparisonExpression.Type.GREATER_THAN, ComparisonExpression.Type.LESS_THAN)
            .put(ComparisonExpression.Type.LESS_THAN, ComparisonExpression.Type.GREATER_THAN)
            .put(ComparisonExpression.Type.GREATER_THAN_OR_EQUAL, ComparisonExpression.Type.LESS_THAN_OR_EQUAL)
            .put(ComparisonExpression.Type.LESS_THAN_OR_EQUAL, ComparisonExpression.Type.GREATER_THAN_OR_EQUAL)
            .build();


    @Override
    protected Symbol visitFunctionCall(FunctionCall node, T context) {
        List<Symbol> arguments = new ArrayList<>(node.getArguments().size());
        List<DataType> argumentTypes = new ArrayList<>(node.getArguments().size());
        for (Expression expression : node.getArguments()) {
            Symbol argSymbol = expression.accept(this, context);

            // cast parameter to literal, guess its type
            if (argSymbol.symbolType() == SymbolType.PARAMETER) {
                argSymbol = ((Parameter) argSymbol).toLiteral();
            }
            ValueSymbol vs = (ValueSymbol) argSymbol;
            arguments.add(vs);
            argumentTypes.add(vs.valueType());
        }

        FunctionInfo functionInfo;
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

        Set<Object> inListSet = new HashSet<>();
        for (Expression expression : ((InListExpression)node.getValueList()).getValues()) {
            Symbol inListValue = expression.accept(this, context);
            Literal literal;
            try {
                if (value instanceof Literal) {
                    literal = context.normalizeInputForType(inListValue, valueDataType);
                } else if (value instanceof Reference) {
                    literal = context.normalizeInputForReference(inListValue, (Reference) value);
                } else {
                    throw new Exception();
                }
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "invalid IN LIST value %s. expected type '%s'",
                                SymbolFormatter.format(inListValue),
                                valueDataType.getName()));
            }
            inListSet.add(literal.value());
        }
        Literal inListSymbol = new SetLiteral(valueDataType, inListSet);

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
                new FunctionIdent(io.crate.operation.predicate.IsNullPredicate.NAME, ImmutableList.of(argumentType));
        FunctionInfo isNullInfo = context.getFunctionInfo(isNullIdent);

        return context.allocateFunction(
                NotPredicate.INFO,
                ImmutableList.<Symbol>of(context.allocateFunction(isNullInfo, ImmutableList.of(argument))));
    }

    @Override
    protected Symbol visitSubscriptExpression(SubscriptExpression node, T context) {
        SubscriptContext subscriptContext = new SubscriptContext();
        node.accept(visitor, subscriptContext);
        ReferenceIdent ident = new ReferenceIdent(
                Objects.firstNonNull(subscriptContext.tableIdent(), context.table().ident()),
                subscriptContext.column(),
                subscriptContext.parts()
        );
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
        Symbol argument = process(node.getValue(), context);
        if (argument.symbolType() == SymbolType.PARAMETER) {
            argument = ((Parameter) argument).toLiteral();
        }
        return new Function(NotPredicate.INFO, Arrays.asList(argument));
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
        // and eq(?, name) to eq(name, ?)
        if (arguments.get(0).symbolType().isLiteral() || arguments.get(0).symbolType() == SymbolType.PARAMETER &&
                (arguments.get(1).symbolType() == SymbolType.REFERENCE ||
                        arguments.get(1).symbolType() == SymbolType.DYNAMIC_REFERENCE)) {
            if (swapOperatorTable.containsKey(node.getType())) {
                operatorName = "op_" + swapOperatorTable.get(node.getType()).getValue();
            }
            Collections.reverse(arguments);
            Collections.reverse(argumentTypes);
        }

        // currently there will be no result for dynamic references, so return here
        if (arguments.get(0).symbolType() == SymbolType.DYNAMIC_REFERENCE){
            return Null.INSTANCE;
        }

        // try implicit type cast (conversion)
        if (arguments.get(1).symbolType() == SymbolType.PARAMETER) {
            switch (arguments.get(0).symbolType()) {
                case PARAMETER:
                    // ? = ? - can only guess types
                    for (int i=0; i<arguments.size();i++) {
                        Literal l = ((Parameter)arguments.get(i)).toLiteral();
                        arguments.set(i, l);
                        argumentTypes.set(i, l.valueType());
                    }
                    break;
                case REFERENCE:
                case DYNAMIC_REFERENCE:
                    // Reference = ?
                    Literal normalized = context.normalizeInputForReference(arguments.get(1), (Reference)arguments.get(0));
                    arguments.set(1, normalized);
                    argumentTypes.set(1, normalized.valueType());
                    break;
            }
        } else if (argumentTypes.get(0) != argumentTypes.get(1)) {
            Symbol convertedSymbol = context.normalizeInputForType(arguments.get(1), argumentTypes.get(0));
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
    public Symbol visitArrayComparisonExpression(ArrayComparisonExpression node, T context) {

        if (node.quantifier().equals(ArrayComparisonExpression.Quantifier.ALL)) {
            throw new IllegalArgumentException("ALL_OF is not supported");
        }

        // resolve arguments
        // implicitly swapping arguments so we got 1. reference, 2. literal
        List<Symbol> arguments = new ArrayList<>(2);
        arguments.add(process(node.getRight(), context));
        arguments.add(process(node.getLeft(), context));

        // resolve argument types
        List<DataType> argumentTypes = new ArrayList<>(arguments.size());
        argumentTypes.add(symbolDataTypeVisitor.process(arguments.get(0), null));
        argumentTypes.add(symbolDataTypeVisitor.process(arguments.get(1), null));

        // check that right type is array or set type
        if (!argumentTypes.get(0).isCollectionType()) {
            throw new IllegalArgumentException(
                    SymbolFormatter.format("invalid array expression: '%s'",
                            arguments.get(0))
            );
        } else if (argumentTypes.get(0).elementType().equals(DataType.OBJECT)) {
            // TODO: remove this artificial limitation in general
            throw new IllegalArgumentException("ANY_OF on object arrays is not supported");
        }

        // check that left type is element type of right array expression
        // if not try implicit type cast
        if (!argumentTypes.get(0).elementType().equals(argumentTypes.get(1))) {

            // try implicit type cast (conversion)
            if (arguments.get(1).symbolType() == SymbolType.PARAMETER) {
                switch (arguments.get(0).symbolType()) {
                    case PARAMETER:
                        // ? = ? - can only guess types
                        for (int i = 0; i < arguments.size(); i++) {
                            Literal l = ((Parameter) arguments.get(i)).toLiteral();
                            arguments.set(i, l);
                            argumentTypes.set(i, l.valueType());
                        }
                        break;
                    case REFERENCE:
                        Literal normalizedRef = context.normalizeInputForType(arguments.get(1),
                                ((Reference)arguments.get(0)).valueType().elementType());
                        arguments.set(1, normalizedRef);
                        argumentTypes.set(1, normalizedRef.valueType());
                        break;
                    case DYNAMIC_REFERENCE:
                        // Reference = ?
                        Literal normalizedDynRef = context.normalizeInputForReference(arguments.get(1),
                                (Reference) arguments.get(0));
                        arguments.set(1, normalizedDynRef);
                        argumentTypes.set(1, normalizedDynRef.valueType());
                        break;
                }
            } else {
                Symbol convertedSymbol = context.normalizeInputForType(arguments.get(1), argumentTypes.get(0).elementType());
                arguments.set(1, convertedSymbol);
                argumentTypes.set(1, argumentTypes.get(0).elementType());
            }
        }

        ComparisonExpression.Type operationType = node.getType();
        String operatorName;
        if (swapOperatorTable.containsKey(operationType)) {
            operatorName = AnyOperator.OPERATOR_PREFIX + swapOperatorTable.get(operationType).getValue();
        } else {
            operatorName = AnyOperator.OPERATOR_PREFIX + operationType.getValue();
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
        Symbol expressionSymbol = process(node.getValue(), context);
        Symbol patternSymbol = process(node.getPattern(), context);

        // handle possible parameter for expression
        // try implicit conversion
        if (! (expressionSymbol instanceof Reference)) {
            try {
                expressionSymbol = context.normalizeInputForType(expressionSymbol, DataType.STRING);
            } catch (IllegalArgumentException|UnsupportedOperationException e) {
                throw new UnsupportedOperationException("<expression> LIKE <pattern>: expression couldn't be implicitly casted to string. Try to explicitly cast to string.");
            }
        }

        // handle possible parameter for pattern
        // try implicit conversion
        try {
            if (expressionSymbol instanceof Reference) {
                patternSymbol = context.normalizeInputForReference(patternSymbol, (Reference) expressionSymbol);
            } else {
                patternSymbol = context.normalizeInputForType(patternSymbol, DataType.STRING);
            }
        } catch (IllegalArgumentException|UnsupportedOperationException e) {
            throw new UnsupportedOperationException("<expression> LIKE <pattern>: pattern couldn't be implicitly casted to string. Try to explicitly cast to string.");
        }

        arguments.add(expressionSymbol);
        arguments.add(patternSymbol);

        // resolve argument types
        List<DataType> argumentTypes = new ArrayList<>(arguments.size());
        argumentTypes.add(symbolDataTypeVisitor.process(arguments.get(0), null));
        argumentTypes.add(symbolDataTypeVisitor.process(arguments.get(1), null));

        // catch null types, might be null or dynamic reference, which are both not supported
        if (argumentTypes.get(0) == DataType.NULL){
            return Null.INSTANCE;
        }

        FunctionInfo functionInfo;
        try {
            // be optimistic. try to look up LikeOperator.
            FunctionIdent functionIdent = new FunctionIdent(LikeOperator.NAME, argumentTypes);
            functionInfo = context.getFunctionInfo(functionIdent);
        } catch (UnsupportedOperationException e1) {
            // check pattern
            if (!(arguments.get(1).symbolType().isLiteral())) {
                throw new UnsupportedOperationException("<expression> LIKE <pattern>: pattern must not be a reference.");
            }
            throw new UnsupportedOperationException("invalid LIKE clause");
        }

        return context.allocateFunction(functionInfo, arguments);
    }


    @Override
    protected Symbol visitIsNullPredicate(IsNullPredicate node, T context) {

        Symbol value = process(node.getValue(), context);

        // currently there will be no result for dynamic references, so return here
        if (value.symbolType() == SymbolType.DYNAMIC_REFERENCE){
            return Null.INSTANCE;
        }

        FunctionIdent functionIdent =
                new FunctionIdent(io.crate.operation.predicate.IsNullPredicate.NAME,
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
            if (!context.sysExpressionsAllowed && context.hasSysExpressions) {
                throw new UnsupportedOperationException("Filtering system columns is currently " +
                        "only supported by queries using group-by or global aggregates.");
            }

            PrimaryKeyVisitor.Context pkc = primaryKeyVisitor.process(context, whereClause.query());
            if (pkc != null) {
                if (pkc.hasPartitionedColumn) {
                    // query was modified, normalize it again
                    whereClause = new WhereClause(pkc.whereClause());
                    whereClause = context.whereClause(whereClause);
                }
                whereClause.clusteredByLiteral(pkc.clusteredByLiteral());
                if (pkc.noMatch) {
                    context.whereClause(WhereClause.NO_MATCH);
                } else {
                    whereClause.version(pkc.version());

                    if (pkc.keyLiterals() != null) {
                        processPrimaryKeyLiterals(pkc.keyLiterals(), context);
                    }

                    if (pkc.partitionLiterals() != null) {
                        whereClause.partitions(pkc.partitionLiterals());
                    }
                }
            }
        }
    }

    protected void processPrimaryKeyLiterals(List primaryKeyLiterals, T context) {
        List<List<String>> primaryKeyValuesList = new ArrayList<>(primaryKeyLiterals.size());
        primaryKeyValuesList.add(new ArrayList<String>(context.table().primaryKey().size()));

        for (int i=0; i<primaryKeyLiterals.size(); i++) {
            Object primaryKey = primaryKeyLiterals.get(i);

            if (primaryKey instanceof Literal) {
                // support e.g. pk IN (Value..,)
                if (((Literal)primaryKey).symbolType() == SymbolType.SET_LITERAL) {
                    Set<Literal> literals = ((SetLiteral) primaryKey).literals();
                    Iterator<Literal> literalIterator = literals.iterator();
                    for (int s=0; s<literals.size(); s++) {
                        Literal pk = literalIterator.next();
                        if (s >= primaryKeyValuesList.size()) {
                            // copy already parsed pk values, so we have all possible multiple pk for all sets
                            primaryKeyValuesList.add(Lists.newArrayList(primaryKeyValuesList.get(s - 1)));
                        }
                        List<String> primaryKeyValues = primaryKeyValuesList.get(s);
                        if (primaryKeyValues.size() > i) {
                            primaryKeyValues.set(i, pk.valueAsString());
                        } else {
                            primaryKeyValues.add(pk.valueAsString());
                        }
                    }
                } else {
                    for (List<String> primaryKeyValues : primaryKeyValuesList) {
                        primaryKeyValues.add(((Literal)primaryKey).valueAsString());
                    }
                }
            } else if (primaryKey instanceof List) {
                primaryKey = Lists.transform((List<Literal>) primaryKey, new com.google.common.base.Function<Literal, String>() {
                    @Override
                    public String apply(Literal input) {
                        return input.valueAsString();
                    }
                });
                primaryKeyValuesList.add((List<String>) primaryKey);
            }
        }

        for (List<String> primaryKeyValues : primaryKeyValuesList) {
            context.addIdAndRouting(primaryKeyValues, context.whereClause().clusteredBy().orNull());
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
        return new Parameter(context.parameterAt(node.index()));
    }
}
