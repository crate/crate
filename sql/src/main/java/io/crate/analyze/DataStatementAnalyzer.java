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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import io.crate.PartitionName;
import io.crate.analyze.expressions.ExpressionToNumberVisitor;
import io.crate.analyze.expressions.ExpressionToObjectVisitor;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.*;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.CollectSetAggregation;
import io.crate.operation.operator.*;
import io.crate.operation.operator.any.AnyLikeOperator;
import io.crate.operation.operator.any.AnyNotLikeOperator;
import io.crate.operation.operator.any.AnyOperator;
import io.crate.operation.predicate.NotPredicate;
import io.crate.operation.reference.partitioned.PartitionExpression;
import io.crate.operation.scalar.SubscriptFunction;
import io.crate.operation.scalar.cast.CastFunctionResolver;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import io.crate.planner.symbol.Literal;
import io.crate.sql.tree.*;
import io.crate.types.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;

import java.util.*;


/**
 * Replaced with {@link io.crate.analyze.expressions.ExpressionAnalyzer}
 */
@Deprecated
abstract class DataStatementAnalyzer<T extends AbstractDataAnalyzedStatement> extends AbstractStatementAnalyzer<Symbol, T> {

    private final static Map<ComparisonExpression.Type, ComparisonExpression.Type> swapOperatorTable = ImmutableMap.<ComparisonExpression.Type, ComparisonExpression.Type>builder()
            .put(ComparisonExpression.Type.GREATER_THAN, ComparisonExpression.Type.LESS_THAN)
            .put(ComparisonExpression.Type.LESS_THAN, ComparisonExpression.Type.GREATER_THAN)
            .put(ComparisonExpression.Type.GREATER_THAN_OR_EQUAL, ComparisonExpression.Type.LESS_THAN_OR_EQUAL)
            .put(ComparisonExpression.Type.LESS_THAN_OR_EQUAL, ComparisonExpression.Type.GREATER_THAN_OR_EQUAL)
            .build();

    private final static String _SCORE = "_score";
    private final static DataTypeAnalyzer DATA_TYPE_ANALYZER = new DataTypeAnalyzer();
    private final static NegativeLiteralVisitor NEGATIVE_LITERAL_VISITOR = new NegativeLiteralVisitor();
    private final static SubscriptVisitor SUBSCRIPT_VISITOR = new SubscriptVisitor();
    private final static PrimaryKeyVisitor PRIMARY_KEY_VISITOR = new PrimaryKeyVisitor();

    @Override
    protected Symbol visitFunctionCall(FunctionCall node, T context) {
        List<Symbol> arguments = new ArrayList<>(node.getArguments().size());
        List<DataType> argumentTypes = new ArrayList<>(node.getArguments().size());
        for (Expression expression : node.getArguments()) {
            Symbol argSymbol = expression.accept(this, context);

            argumentTypes.add(argSymbol.valueType());
            arguments.add(argSymbol);
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

            // define the outer function which contains the inner function as argument.
            String nodeName = "collection_" + node.getName().toString();
            List<Symbol> outerArguments = Arrays.<Symbol>asList(innerFunction);
            ImmutableList<DataType> outerArgumentTypes =
                    ImmutableList.<DataType>of(new SetType(argumentTypes.get(0)));

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
    protected Symbol visitCast(Cast node, T context) {
        DataType returnType = DATA_TYPE_ANALYZER.process(node.getType(), null);
        Symbol argSymbol = node.getExpression().accept(this, context);
        FunctionInfo functionInfo = CastFunctionResolver.functionInfo(argSymbol.valueType(), returnType);
        return context.allocateFunction(functionInfo, Arrays.asList(argSymbol));
    }

    @Override
    protected Symbol visitInPredicate(InPredicate node, T context) {
        Symbol left = process(node.getValue(), context);
        DataType leftType = left.valueType();
        if (leftType.equals(DataTypes.UNDEFINED)) {
            // dynamic or null values cannot be queried, in scalar use-cases (system tables)
            // we currently have no dynamics
            return Literal.NULL;
        }
        validateSystemColumnPredicate(left);

        Set<Object> rightValues = new HashSet<>();
        for (Expression expression : ((InListExpression)node.getValueList()).getValues()) {
            Symbol right = expression.accept(this, context);
            Literal rightLiteral;
            try {
                rightLiteral = Literal.convert(right, leftType);
                rightValues.add(rightLiteral.value());
            } catch (IllegalArgumentException | ClassCastException e) {
                   throw new IllegalArgumentException(
                           String.format(Locale.ENGLISH, "invalid IN LIST value %s. expected type '%s'",
                                   SymbolFormatter.format(right),
                                   leftType.getName()));
            }
        }
        SetType setType = new SetType(leftType);
        FunctionIdent functionIdent = new FunctionIdent(InOperator.NAME, Arrays.asList(leftType, setType));
        FunctionInfo functionInfo = context.getFunctionInfo(functionIdent);
        return context.allocateFunction(
                functionInfo,
                Arrays.asList(left, Literal.newLiteral(setType, rightValues)));
    }

    @Override
    protected Symbol visitAliasedRelation(AliasedRelation node, T context) {
        process(node.getRelation(), context);
        context.tableAlias(node.getAlias());
        return null;
    }

    @Override
    protected Symbol visitIsNotNullPredicate(IsNotNullPredicate node, T context) {
        Symbol argument = process(node.getValue(), context);
        validateSystemColumnPredicate(argument);

        FunctionIdent isNullIdent =
                new FunctionIdent(io.crate.operation.predicate.IsNullPredicate.NAME, ImmutableList.of(argument.valueType()));
        FunctionInfo isNullInfo = context.getFunctionInfo(isNullIdent);

        return context.allocateFunction(
                NotPredicate.INFO,
                Arrays.<Symbol>asList(context.allocateFunction(isNullInfo, Arrays.asList(argument))));
    }

    @Override
    protected Symbol visitSubscriptExpression(SubscriptExpression node, T context) {
        SubscriptContext subscriptContext = new SubscriptContext(context.parameterContext());
        node.accept(SUBSCRIPT_VISITOR, subscriptContext);
        return resolveSubscriptSymbol(subscriptContext, context);
    }

    protected Symbol resolveSubscriptSymbol(SubscriptContext subscriptContext, T context) {
        // TODO: support nested subscripts as soon as DataTypes.OBJECT elements can be typed
        Symbol subscriptSymbol;
        Expression subscriptExpression = subscriptContext.expression();
        if (subscriptContext.qName() != null && subscriptExpression == null) {
            ReferenceIdent ident = context.getReference(
                    subscriptContext.qName(),
                    subscriptContext.parts()
            );
            subscriptSymbol = context.allocateReference(ident);
        } else if (subscriptExpression != null) {
            subscriptSymbol = subscriptExpression.accept(this, context);
        } else {
            throw new UnsupportedOperationException("Only references, function calls or array literals " +
                                                    "are valid subscript symbols");
        }
        if (subscriptContext.index() != null) {
            // rewrite array access to subscript scalar
            FunctionIdent functionIdent = new FunctionIdent(SubscriptFunction.NAME,
                    ImmutableList.of(subscriptSymbol.valueType(), DataTypes.INTEGER));
            return context.allocateFunction(context.getFunctionInfo(functionIdent),
                    Arrays.asList(subscriptSymbol, (Symbol) Literal.newLiteral(subscriptContext.index())));
        }
        return subscriptSymbol;
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
        context.insideNotPredicate = true;
        Symbol argument = process(node.getValue(), context);
        context.insideNotPredicate = false;

        DataType dataType = argument.valueType();
        if (!dataType.equals(DataTypes.BOOLEAN) && !dataType.equals(DataTypes.UNDEFINED)) {
            throw new IllegalArgumentException(String.format(
                "Invalid argument of type \"%s\" passed to %s predicate. Argument must resolve to boolean or null",
                dataType,
                node
            ));
        }
        return new Function(NotPredicate.INFO, Arrays.asList(argument));
    }

    @Override
    protected Symbol visitComparisonExpression(ComparisonExpression node, T context) {
        Symbol left = process(node.getLeft(), context);
        Symbol right = process(node.getRight(), context);
        if (left.symbolType() == SymbolType.DYNAMIC_REFERENCE || right.symbolType() == SymbolType.DYNAMIC_REFERENCE) {
            return Literal.NULL;
        }
        Comparison comparison = new Comparison(node.getType(), left, right);
        comparison.normalize(context);
        validateSystemColumnComparison(comparison, context);
        FunctionInfo info = context.getFunctionInfo(comparison.toFunctionIdent());
        return context.allocateFunction(info, comparison.arguments());
    }

    /**
     * Validates comparison of system columns like e.g. '_score'.
     * Must be called AFTER comparison normalization.
     */
    protected void validateSystemColumnComparison(Comparison comparison, T context) {
        if (comparison.left.symbolType() == SymbolType.REFERENCE) {
            Reference reference = (Reference) comparison.left;
            // _score column can only be used by > comparator
            if (reference.info().ident().columnIdent().name().equalsIgnoreCase(_SCORE)
                    && (comparison.comparisonExpressionType != ComparisonExpression.Type.GREATER_THAN_OR_EQUAL
                        || context.insideNotPredicate)) {
                throw new UnsupportedOperationException(
                        String.format(Locale.ENGLISH,
                                "System column '%s' can only be used within a '%s' comparison without any surrounded predicate",
                                _SCORE, ComparisonExpression.Type.GREATER_THAN_OR_EQUAL.getValue()));
            }
        }
    }

    /**
     * Validates usage of system columns (e.g. '_score') within predicates.
     */
    protected void validateSystemColumnPredicate(Symbol node) {
        if (node.symbolType() == SymbolType.REFERENCE) {
            Reference reference = (Reference) node;
            if (reference.info().ident().columnIdent().name().equalsIgnoreCase(_SCORE)) {
                throw new UnsupportedOperationException(
                        String.format(Locale.ENGLISH,
                                "System column '%s' cannot be used within a predicate", _SCORE));
            }
        }
    }

    @Override
    public Symbol visitArrayComparisonExpression(ArrayComparisonExpression node, T context) {
        if (node.quantifier().equals(ArrayComparisonExpression.Quantifier.ALL)) {
            throw new UnsupportedFeatureException("ALL is not supported");
        }

        // implicitly swapping arguments so we got 1. reference, 2. literal
        Symbol left = process(node.getRight(), context);
        Symbol right = process(node.getLeft(), context);
        DataType leftType = left.valueType();

        if (! DataTypes.isCollectionType(leftType)) {
            throw new IllegalArgumentException(
                    SymbolFormatter.format("invalid array expression: '%s'", left));
        }
        assert leftType instanceof CollectionType;

        DataType leftInnerType = ((CollectionType)leftType).innerType();
        if (leftInnerType.equals(DataTypes.OBJECT)) {
            throw new IllegalArgumentException("ANY on object arrays is not supported");
        }

        if (right.symbolType().isValueSymbol()) {
            right = Literal.convert(right, leftInnerType);
        } else {
            throw new IllegalArgumentException(
                    "The left side of an ANY comparison must be a value, not a column reference");
        }
        ComparisonExpression.Type operationType = node.getType();
        String operatorName;
        if (swapOperatorTable.containsKey(operationType)) {
            operatorName = AnyOperator.OPERATOR_PREFIX + swapOperatorTable.get(operationType).getValue();
        } else {
            operatorName = AnyOperator.OPERATOR_PREFIX + operationType.getValue();
        }
        FunctionIdent functionIdent = new FunctionIdent(
                operatorName, Arrays.asList(leftType, ((Literal)right).valueType()));
        FunctionInfo functionInfo = context.getFunctionInfo(functionIdent);
        return context.allocateFunction(functionInfo, Arrays.asList(left, right));
    }

    @Override
    public Symbol visitArrayLikePredicate(ArrayLikePredicate node, T context) {
        if (node.getEscape() != null) {
            throw new UnsupportedOperationException("ESCAPE is not supported.");
        }
        Symbol left = process(node.getValue(), context);
        Symbol right = process(node.getPattern(), context);
        DataType leftType = left.valueType();

        if (! DataTypes.isCollectionType(leftType)) {
            throw new IllegalArgumentException(
                    SymbolFormatter.format("invalid array expression: '%s'", left));
        }

        DataType leftInnerType = ((CollectionType)leftType).innerType();
        if (leftInnerType.id() != StringType.ID) {
            if (!(left instanceof Reference)) {
                left = context.normalizeInputForType(left, new ArrayType(DataTypes.STRING));
            } else {
                throw new IllegalArgumentException(
                        SymbolFormatter.format("elements not of type string: '%s'", left));
            }
        }

        if (right.symbolType().isValueSymbol()) {
            right = context.normalizeInputForType(right, leftInnerType);
        } else {
            throw new IllegalArgumentException(
                    "The left side of an ANY comparison must be a value, not a column reference");
        }
        String operatorName = node.inverse() ? AnyNotLikeOperator.NAME : AnyLikeOperator.NAME;

        FunctionIdent functionIdent = new FunctionIdent(
                operatorName, Arrays.asList(leftType, ((Literal)right).valueType()));
        FunctionInfo functionInfo = context.getFunctionInfo(functionIdent);
        return context.allocateFunction(functionInfo, Arrays.asList(left, right));
    }

    @Override
    protected Symbol visitLikePredicate(LikePredicate node, T context) {
        if (node.getEscape() != null) {
            throw new UnsupportedOperationException("ESCAPE is not supported yet.");
        }

        Symbol expressionSymbol = process(node.getValue(), context);
        Symbol patternSymbol = process(node.getPattern(), context);

        // handle possible parameter for expression
        // try implicit conversion
        if (! (expressionSymbol instanceof Reference)) {
            try {
                expressionSymbol = context.normalizeInputForType(expressionSymbol, StringType.INSTANCE);
            } catch (IllegalArgumentException|UnsupportedOperationException e) {
                throw new UnsupportedOperationException("<expression> LIKE <pattern>: expression couldn't be implicitly casted to string. Try to explicitly cast to string.");
            }
        }

        DataType expressionType = expressionSymbol.valueType();
        DataType patternType;
        try {
            if (expressionSymbol instanceof Reference) {
                patternSymbol = context.normalizeInputForReference(patternSymbol, (Reference) expressionSymbol);
                patternType = patternSymbol.valueType();
            } else {
                patternSymbol = context.normalizeInputForType(patternSymbol, DataTypes.STRING);
                patternType = DataTypes.STRING;
            }
        } catch (IllegalArgumentException|UnsupportedOperationException e) {
            throw new UnsupportedOperationException("<expression> LIKE <pattern>: pattern couldn't be implicitly casted to string. Try to explicitly cast to string.");
        }

        // catch null types, might be null or dynamic reference, which are both not supported
       if (expressionType.equals(DataTypes.UNDEFINED)) {
           return Literal.NULL;
       }

        FunctionInfo functionInfo;
        try {
            FunctionIdent functionIdent = new FunctionIdent(LikeOperator.NAME, Arrays.asList(expressionType, patternType));
            functionInfo = context.getFunctionInfo(functionIdent);
        } catch (UnsupportedOperationException e1) {
            // check pattern
            if (!(patternSymbol.symbolType().isValueSymbol())) {
                throw new UnsupportedOperationException("<expression> LIKE <pattern>: pattern must not be a reference.");
            }
            throw new UnsupportedOperationException("invalid LIKE clause");
        }
        return context.allocateFunction(functionInfo, Arrays.asList(expressionSymbol, patternSymbol));
    }


    @Override
    protected Symbol visitIsNullPredicate(IsNullPredicate node, T context) {
        Symbol value = process(node.getValue(), context);

        // currently there will be no result for dynamic references, so return here
        if (value.symbolType() == SymbolType.DYNAMIC_REFERENCE){
            return Literal.NULL;
        }
        validateSystemColumnPredicate(value);

        FunctionIdent functionIdent =
                new FunctionIdent(io.crate.operation.predicate.IsNullPredicate.NAME,  ImmutableList.of(value.valueType()));
        FunctionInfo functionInfo = context.getFunctionInfo(functionIdent);
        return context.allocateFunction(functionInfo, Arrays.asList(value));
    }

    @Override
    protected Symbol visitNegativeExpression(NegativeExpression node, T context) {
        // in statements like "where x = -1" the  positive (expression)IntegerLiteral (1)
        // is just wrapped inside a negativeExpression
        // the visitor here swaps it to get -1 in a (symbol)LiteralInteger
        return NEGATIVE_LITERAL_VISITOR.process(process(node.getValue(), context), null);
    }

    protected WhereClause generateWhereClause(Optional<Expression> whereExpression, T context) {
        if (!whereExpression.isPresent()) {
            return WhereClause.MATCH_ALL;
        }

        WhereClause whereClause = new WhereClause(
                context.normalizer.normalize(process(whereExpression.get(), context)));
        if (whereClause.hasQuery()){
            if (!context.sysExpressionsAllowed && context.hasSysExpressions) {
                throw new UnsupportedOperationException("Filtering system columns is currently " +
                        "only supported by queries using group-by or global aggregates.");
            }

            PrimaryKeyVisitor.Context pkc = PRIMARY_KEY_VISITOR.process(context.table(), whereClause.query());
            if (pkc != null) {
                whereClause.clusteredByLiteral(pkc.clusteredByLiteral());
                if (pkc.noMatch) {
                    whereClause = WhereClause.NO_MATCH;
                } else {
                    whereClause.version(pkc.version());

                    if (pkc.keyLiterals() != null) {
                        processPrimaryKeyLiterals(pkc.keyLiterals(), whereClause, context);
                    }
                }
            }

            // TODO: this should be part of the getRouting on tableInfo
            if (context.table().isPartitioned()) {
                whereClause = resolvePartitions(
                        context.referenceResolver,
                        context.functions, whereClause, context.table());
            }
        }
        return whereClause;
    }

    private PartitionReferenceResolver preparePartitionResolver(
            ReferenceResolver referenceResolver, List<ReferenceInfo> partitionColumns) {
        List<PartitionExpression> partitionExpressions = new ArrayList<>(partitionColumns.size());
        int idx = 0;
        for (ReferenceInfo partitionedByColumn : partitionColumns) {
            partitionExpressions.add(new PartitionExpression(partitionedByColumn, idx));
            idx++;
        }
        return new PartitionReferenceResolver(referenceResolver, partitionExpressions);
    }

    private WhereClause resolvePartitions(ReferenceResolver referenceResolver,
                                          Functions functions,
                                          WhereClause whereClause,
                                          TableInfo table) {
        assert table.isPartitioned() : "table must be partitioned in order to resolve partitions";
        if (table.partitions().isEmpty()) {
            return WhereClause.NO_MATCH; // table is partitioned but has no data / no partitions
        }
        PartitionReferenceResolver partitionReferenceResolver = preparePartitionResolver(
                referenceResolver,
                table.partitionedByColumns());
        EvaluatingNormalizer normalizer =
                new EvaluatingNormalizer(functions, RowGranularity.PARTITION, partitionReferenceResolver);

        Symbol normalized = null;
        Map<Symbol, List<Literal>> queryPartitionMap = new HashMap<>();

        for (PartitionName partitionName : table.partitions()) {
            for (PartitionExpression partitionExpression : partitionReferenceResolver.expressions()) {
                partitionExpression.setNextRow(partitionName);
            }
            normalized = normalizer.normalize(whereClause.query());
            assert normalized != null : "normalizing a query must not return null";

            if (normalized.equals(whereClause.query())) {
                return whereClause; // no partition columns inside the where clause
            }

            boolean canMatch = WhereClause.canMatch(normalized);
            if (canMatch) {
                List<Literal> partitions = queryPartitionMap.get(normalized);
                if (partitions == null) {
                    partitions = new ArrayList<>();
                    queryPartitionMap.put(normalized, partitions);
                }
                partitions.add(Literal.newLiteral(partitionName.stringValue()));
            }
        }

        if (queryPartitionMap.size() == 1) {
            Map.Entry<Symbol, List<Literal>> entry = queryPartitionMap.entrySet().iterator().next();
            whereClause = new WhereClause(entry.getKey());
            whereClause.partitions(entry.getValue());
        } else if (queryPartitionMap.size() > 0) {
            whereClause = tieBreakPartitionQueries(normalizer, queryPartitionMap);
        } else {
            whereClause = WhereClause.NO_MATCH;
        }

        return whereClause;
    }

    private WhereClause tieBreakPartitionQueries(EvaluatingNormalizer normalizer,
                                                 Map<Symbol, List<Literal>> queryPartitionMap) throws UnsupportedOperationException{
        /**
         * Got multiple normalized queries which all could match.
         * This might be the case if one partition resolved to null
         *
         * e.g.
         *
         *  p = 1 and x = 2
         *
         * might lead to
         *
         *  null and x = 2
         *  true and x = 2
         *
         * At this point it is unknown if they really match.
         * In order to figure out if they could potentially match all conditions involving references are now set to true
         *
         *  null and true   -> can't match
         *  true and true   -> can match, can use this query + partition
         *
         * If there is still more than 1 query that can match it's not possible to execute the query :(
         */

        List<Tuple<Symbol, List<Literal>>> canMatch = new ArrayList<>();
        ReferenceToTrueVisitor referenceToTrueVisitor = new ReferenceToTrueVisitor();
        for (Map.Entry<Symbol, List<Literal>> entry : queryPartitionMap.entrySet()) {
            Symbol query = entry.getKey();
            List<Literal> partitions = entry.getValue();

            Symbol symbol = referenceToTrueVisitor.process(query, null);
            Symbol normalized = normalizer.normalize(symbol);

            assert normalized instanceof Literal && ((Literal) normalized).valueType().equals(DataTypes.BOOLEAN) :
                "after normalization and replacing all reference occurrences with true there must only be a boolean left";

            Object value = ((Literal) normalized).value();
            if (value != null && (Boolean) value) {
                canMatch.add(new Tuple<>(query, partitions));
            }
        }
        if (canMatch.size() == 1) {
            Tuple<Symbol, List<Literal>> symbolListTuple = canMatch.get(0);
            WhereClause whereClause = new WhereClause(symbolListTuple.v1());
            whereClause.partitions(symbolListTuple.v2());
            return whereClause;
        }
        throw new UnsupportedOperationException(
            "logical conjunction of the conditions in the WHERE clause which " +
                "involve partitioned columns led to a query that can't be executed.");
    }

    protected void processPrimaryKeyLiterals(List primaryKeyLiterals, WhereClause whereClause, T context) {
        List<List<BytesRef>> primaryKeyValuesList = new ArrayList<>(primaryKeyLiterals.size());
        primaryKeyValuesList.add(new ArrayList<BytesRef>(context.table().primaryKey().size()));

        for (int i=0; i<primaryKeyLiterals.size(); i++) {
            Object primaryKey = primaryKeyLiterals.get(i);
            if (primaryKey instanceof Literal) {
                Literal pkLiteral = (Literal)primaryKey;
                // support e.g. pk IN (Value..,)
                if (pkLiteral.valueType().id() == SetType.ID) {
                    Set<Literal> literals = Sets.newHashSet(Literal.explodeCollection(pkLiteral));
                    Iterator<Literal> literalIterator = literals.iterator();
                    for (int s=0; s<literals.size(); s++) {
                        Literal pk = literalIterator.next();
                        if (s >= primaryKeyValuesList.size()) {
                            // copy already parsed pk values, so we have all possible multiple pk for all sets
                            primaryKeyValuesList.add(Lists.newArrayList(primaryKeyValuesList.get(s - 1)));
                        }
                        List<BytesRef> primaryKeyValues = primaryKeyValuesList.get(s);
                        if (primaryKeyValues.size() > i) {
                            primaryKeyValues.set(i, BytesRefValueSymbolVisitor.INSTANCE.process(pk));
                        } else {
                            primaryKeyValues.add(BytesRefValueSymbolVisitor.INSTANCE.process(pk));
                        }
                    }
                } else {
                    for (List<BytesRef> primaryKeyValues : primaryKeyValuesList) {
                        primaryKeyValues.add((BytesRefValueSymbolVisitor.INSTANCE.process(pkLiteral)));
                    }
                }
            } else if (primaryKey instanceof List) {
                primaryKey = Lists.transform((List<Literal>) primaryKey, new com.google.common.base.Function<Literal, BytesRef>() {
                    @Override
                    public BytesRef apply(Literal input) {
                        return BytesRefValueSymbolVisitor.INSTANCE.process(input);
                    }
                });
                primaryKeyValuesList.add((List<BytesRef>) primaryKey);
            }
        }

        for (List<BytesRef> primaryKeyValues : primaryKeyValuesList) {
            context.addIdAndRouting(primaryKeyValues, whereClause.clusteredBy().orNull());
        }
    }

    @Override
    protected Symbol visitArithmeticExpression(ArithmeticExpression node, T context) {
        Symbol left = process(node.getLeft(), context);
        Symbol right = process(node.getRight(), context);

        FunctionIdent functionIdent = new FunctionIdent(
                node.getType().name().toLowerCase(Locale.ENGLISH),
                Arrays.asList(left.valueType(), right.valueType())
        );
        return context.allocateFunction(context.getFunctionInfo(functionIdent),  Arrays.asList(left, right));
    }

    @Override
    protected Symbol visitQualifiedNameReference(QualifiedNameReference node, T context) {
        ReferenceIdent ident = context.getReference(node.getName());
        return context.allocateReference(ident);
    }

    @Override
    protected Symbol visitBooleanLiteral(BooleanLiteral node, T context) {
        return Literal.newLiteral(node.getValue());
    }

    @Override
    protected Symbol visitStringLiteral(StringLiteral node, T context) {
        return Literal.newLiteral(node.getValue());
    }

    @Override
    protected Symbol visitDoubleLiteral(DoubleLiteral node, T context) {
        return Literal.newLiteral(node.getValue());
    }

    @Override
    protected Symbol visitLongLiteral(LongLiteral node, T context) {
        return Literal.newLiteral(node.getValue());
    }

    @Override
    protected Symbol visitNullLiteral(NullLiteral node, T context) {
        return Literal.newLiteral(UndefinedType.INSTANCE, null);
    }

    @Override
    public Symbol visitArrayLiteral(ArrayLiteral node, T context) {
        // TODO: support everything that is immediately evaluable as values
        if (node.values().isEmpty()) {
            return Literal.newLiteral(new ArrayType(UndefinedType.INSTANCE), new Object[0]);
        } else {
            DataType innerType = null;
            List<Literal> literals = new ArrayList<>(node.values().size());
            for (Expression e : node.values()) {
                Symbol arrayElement = process(e, context);
                if (innerType == null) {
                    innerType = arrayElement.valueType();
                } else if (!arrayElement.valueType().equals(innerType)){
                    throw new IllegalArgumentException(String.format(
                            Locale.ENGLISH, "array element %s not of array item type %s",  e, innerType));
                }
                literals.add(Literal.convert(arrayElement, innerType));
            }
            return Literal.implodeCollection(innerType, literals);
        }
    }

    @Override
    public Symbol visitObjectLiteral(ObjectLiteral node, T context) {
        // TODO: support everything that is immediately evaluable as values
        Map<String, Object> values = new HashMap<>(node.values().size());
        for (Map.Entry<String, Expression> entry : node.values().entries()) {
            Object value;
            try {
                value = ExpressionToObjectVisitor.convert(
                        entry.getValue(), context.parameters());
            } catch (UnsupportedOperationException e) {
                throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH,
                                "invalid object literal value '%s'",
                                entry.getValue())
                );
            }

            if (values.put(entry.getKey(), value) != null) {
                throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH,
                                "key '%s' listed twice in object literal",
                                entry.getKey()));
            }
        }
        return Literal.newLiteral(values);
    }

    @Override
    public Symbol visitParameterExpression(ParameterExpression node, T context) {
        return context.parameterContext().getAsSymbol(node.index());
    }

    @Override
    public Symbol visitMatchPredicate(MatchPredicate node, T context) {

        Map<String, Object> identBoostMap = new HashMap<>();
        for (MatchPredicateColumnIdent ident : node.idents()) {
            Symbol reference = process(ident.columnIdent(), context);
            Preconditions.checkArgument(
                reference instanceof Reference,
                SymbolFormatter.format("can only MATCH on references, not on %s", reference)
            );
            Preconditions.checkArgument(
                !(reference instanceof DynamicReference),
                SymbolFormatter.format("cannot MATCH on non existing column %s", reference)
            );
            Number boost = ExpressionToNumberVisitor.convert(ident.boost(), context.parameters());
            identBoostMap.put(((Reference) reference).info().ident().columnIdent().fqn(),
                        boost == null ? null: boost.doubleValue());
        }

        String queryTerm = ExpressionToStringVisitor.convert(node.value(), context.parameters());

        String matchType = node.matchType() == null
                    ? io.crate.operation.predicate.MatchPredicate.DEFAULT_MATCH_TYPE_STRING
                    : node.matchType();
        try {
            MultiMatchQueryBuilder.Type.parse(matchType);
        } catch (ElasticsearchParseException e) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "invalid MATCH type '%s'", matchType), e);
        }

        Map<String, Object> options = MatchOptionsAnalysis.process(node.properties(), context.parameters());

        FunctionInfo functionInfo = context.getFunctionInfo(io.crate.operation.predicate.MatchPredicate.IDENT);
        return context.allocateFunction(functionInfo,
                Arrays.<Symbol>asList(
                        Literal.newLiteral(identBoostMap),
                        Literal.newLiteral(queryTerm),
                        Literal.newLiteral(matchType),
                        Literal.newLiteral(options)));
    }


    private static class Comparison {

        private static final Set<ComparisonExpression.Type> NEGATING_TYPES = ImmutableSet.of(
                ComparisonExpression.Type.REGEX_NO_MATCH,
                ComparisonExpression.Type.NOT_EQUAL);

        private ComparisonExpression.Type comparisonExpressionType;
        private Symbol left;
        private Symbol right;
        private DataType leftType;
        private DataType rightType;
        private String operatorName;
        private FunctionIdent functionIdent = null;

        private Comparison(ComparisonExpression.Type comparisonExpressionType, Symbol left, Symbol right) {
            this.operatorName = "op_" + comparisonExpressionType.getValue();
            this.comparisonExpressionType = comparisonExpressionType;
            this.left = left;
            this.right = right;
            this.leftType = left.valueType();
            this.rightType = right.valueType();
        }

        void normalize(AbstractDataAnalyzedStatement context) {
            swapIfNecessary();
            castTypes();
            rewriteNegatingOperators(context);
        }

        /**
         * swaps the comparison so that references are on the left side.
         * e.g.:
         *      eq(2, name)  becomes  eq(name, 2)
         */
        private void swapIfNecessary() {
            if ( !(left.symbolType().isValueSymbol() &&
                    right.symbolType() == SymbolType.REFERENCE || right.symbolType() == SymbolType.DYNAMIC_REFERENCE)) {
                return;
            }
            ComparisonExpression.Type type = swapOperatorTable.get(comparisonExpressionType);
            if (type != null) {
                comparisonExpressionType = type;
                operatorName = "op_" + type.getValue();
            }
            Symbol tmp = left;
            DataType tmpType = leftType;
            left = right;
            leftType = rightType;
            right = tmp;
            rightType = tmpType;
        }

        private void castTypes() {
            if (leftType == rightType) {
                return;
            }

            if (!left.symbolType().isValueSymbol() && !right.symbolType().isValueSymbol()) {
                // no value symbol -> wrap in cast function because eager cast isn't possible.
                if (rightType.isConvertableTo(leftType)) {
                    FunctionInfo functionInfo = CastFunctionResolver.functionInfo(rightType, leftType);
                    List<Symbol> arguments = Arrays.asList(right);
                    right = new Function(functionInfo, arguments);
                } else {
                    throw new IllegalArgumentException(SymbolFormatter.format(
                            "type of \"%s\" doesn't match type of \"%s\" and cannot be cast implicitly",
                            left,
                            right
                    ));
                }
            } else {
                try {
                    right = Literal.convert(right, leftType);
                } catch (ClassCastException | NumberFormatException e) {
                    throw new IllegalArgumentException(SymbolFormatter.format(
                            "type of \"%s\" doesn't match type of \"%s\" and cannot be cast implicitly",
                            left,
                            right
                    ));
                }
            }
            rightType = leftType;
        }

        /**
         * rewrite   exp1 != exp2  to not(eq(exp1, exp2))
         * and       exp1 !~ exp2  to not(~(exp1, exp2))
         * does nothing if operator != not equals
         */
        private void rewriteNegatingOperators(AbstractDataAnalyzedStatement context) {
            if (!NEGATING_TYPES.contains(comparisonExpressionType)) {
                return;
            }
            String opName = comparisonExpressionType.equals(ComparisonExpression.Type.NOT_EQUAL)
                    ? EqOperator.NAME : RegexpMatchOperator.NAME;
            DataType opType = comparisonExpressionType.equals(ComparisonExpression.Type.NOT_EQUAL)
                    ? EqOperator.RETURN_TYPE : RegexpMatchOperator.RETURN_TYPE;
            FunctionIdent ident = new FunctionIdent(opName, Arrays.asList(leftType, rightType));
            left = context.allocateFunction(
                    new FunctionInfo(ident, opType),
                    Arrays.asList(left, right)
            );
            right = null;
            rightType = null;
            functionIdent = NotPredicate.INFO.ident();
            leftType = opType;
            operatorName = NotPredicate.NAME;
        }

        FunctionIdent toFunctionIdent() {
            if (functionIdent == null) {
                return new FunctionIdent(operatorName, Arrays.asList(leftType, rightType));
            }
            return functionIdent;
        }

        List<Symbol> arguments() {
            if (right == null) {
                 // this is the case if the comparison has been rewritten to not(eq(exp1, exp2))
                return Arrays.asList(left);
            }
            return Arrays.asList(left, right);
        }
    }
}
