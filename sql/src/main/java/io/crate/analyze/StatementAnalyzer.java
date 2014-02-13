package io.crate.analyze;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.*;
import io.crate.operator.operator.*;
import io.crate.planner.symbol.*;
import io.crate.planner.symbol.Literal;
import io.crate.sql.ExpressionFormatter;
import io.crate.sql.tree.*;
import io.crate.sql.tree.DoubleLiteral;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.StringLiteral;
import org.cratedb.DataType;

import java.util.*;

class StatementAnalyzer extends DefaultTraversalVisitor<Symbol, Analysis> {

    private static OutputNameFormatter outputNameFormatter = new OutputNameFormatter();
    protected static SubscriptVisitor visitor = new SubscriptVisitor();
    protected static SymbolDataTypeVisitor symbolDataTypeVisitor = new SymbolDataTypeVisitor();
    protected static NegativeLiteralVisitor negativeLiteralVisitor = new NegativeLiteralVisitor();
    private final Map<String, String> swapOperatorTable = ImmutableMap.<String, String>builder()
            .put(GtOperator.NAME, LtOperator.NAME)
            .put(GteOperator.NAME, LteOperator.NAME)
            .put(LtOperator.NAME, GtOperator.NAME)
            .put(LteOperator.NAME, GteOperator.NAME)
            .build();

    static class OutputNameFormatter extends ExpressionFormatter.Formatter {
        @Override
        protected String visitQualifiedNameReference(QualifiedNameReference node, Void context) {

            List<String> parts = new ArrayList<>();
            for (String part : node.getName().getParts()) {
                parts.add(part);
            }
            return Joiner.on('.').join(parts);
        }

        @Override
        protected String visitSubscriptExpression(SubscriptExpression node, Void context) {
            return String.format("%s[%s]", process(node.name(), null), process(node.index(), null));
        }
    }

    @Override
    protected Symbol visitQuerySpecification(QuerySpecification node, Analysis context) {
        // visit the from first, since this qualifies the select
        if (node.getFrom() != null) {
            for (Relation relation : node.getFrom()) {
                process(relation, context);
            }
        }

        // the parsers sql grammer makes sure that only a integer matches after limit/offset so
        // parseInt can't fail here.
        if (node.getLimit().isPresent()) {
            context.limit(Integer.parseInt(node.getLimit().get()));
        }
        if (node.getOffset().isPresent()) {
            context.offset(Integer.parseInt(node.getOffset().get()));
        }

        // the whereClause shouldn't resolve the aliases so this is done before resolving
        // the result columns to make sure the alias map is empty.
        if (node.getWhere().isPresent()) {
            Function function = (Function) process(node.getWhere().get(), context);
            context.whereClause(function);
        }

        process(node.getSelect(), context);

        if (!node.getGroupBy().isEmpty()) {
            analyzeGroupBy(node.getGroupBy(), context);
        }

        Preconditions.checkArgument(node.getHaving().isPresent() == false, "having clause is not yet supported");

        if (node.getOrderBy().size() > 0) {
            List<Symbol> sortSymbols = new ArrayList<>(node.getOrderBy().size());
            context.reverseFlags(new boolean[node.getOrderBy().size()]);
            int i = 0;
            for (SortItem sortItem : node.getOrderBy()) {

                sortSymbols.add(process(sortItem, context));
                context.reverseFlags()[i++] = sortItem.getOrdering() == SortItem.Ordering.DESCENDING;
            }
            context.sortSymbols(sortSymbols);
        }
        return null;
    }

    private void analyzeGroupBy(List<Expression> groupByExpressions, Analysis context) {
        List<Symbol> groupBy = new ArrayList<>(groupByExpressions.size());
        for (Expression expression : groupByExpressions) {
            Symbol s = process(expression, context);
            int idx;
            if (s.symbolType() == SymbolType.LONG_LITERAL) {
                idx = ((io.crate.planner.symbol.LongLiteral)s).value().intValue() - 1;
                if (idx < 1) {
                    throw new IllegalArgumentException(
                            String.format("GROUP BY position %s is not in select list", idx));
                }
            } else {
                idx = context.outputSymbols().indexOf(s);
            }

            if (idx >= 0) {
                try {
                    s = context.outputSymbols().get(idx);
                } catch (ArrayIndexOutOfBoundsException e) {
                    throw new IllegalArgumentException(
                            String.format("GROUP BY position %s is not in select list", idx));
                }
            }

            if (s.symbolType() == SymbolType.FUNCTION && ((Function)s).info().isAggregate()) {
                throw new IllegalArgumentException("Aggregate functions are not allowed in GROUP BY");
            }

            groupBy.add(s);
        }
        context.groupBy(groupBy);

        ensureOutputSymbolsInGroupBy(context);
    }

    @Override
    protected Symbol visitNegativeExpression(NegativeExpression node, Analysis context) {
        // in statements like "where x = -1" the  positive (expression)IntegerLiteral (1)
        // is just wrapped inside a negativeExpression
        // the visitor here swaps it to get -1 in a (symbol)LiteralInteger
        return negativeLiteralVisitor.process(process(node.getValue(), context), null);
    }

    private void ensureOutputSymbolsInGroupBy(Analysis context) {
        for (Symbol symbol : context.outputSymbols()) {
            if (symbol.symbolType() == SymbolType.FUNCTION && ((Function)symbol).info().isAggregate()) {
                continue;
            }
            if (!context.groupBy().contains(symbol)) {
                throw new IllegalArgumentException(
                        String.format("column %s must appear in the GROUP BY clause or be used in an aggregation function", symbol));
            }
        }
    }

    @Override
    protected Symbol visitSortItem(SortItem node, Analysis context) {
        return super.visitSortItem(node, context);
    }

    @Override
    protected Symbol visitQuery(Query node, Analysis context) {
        context.query(node);
        return super.visitQuery(node, context);
    }

    @Override
    protected Symbol visitNode(Node node, Analysis context) {
        System.out.println("Not analyzed node: " + node);
        return super.visitNode(node, context);
    }

    @Override
    protected Symbol visitSelect(Select node, Analysis context) {
        context.outputSymbols(new ArrayList<Symbol>(node.getSelectItems().size()));
        context.outputNames(new ArrayList<String>(node.getSelectItems().size()));

        for (SelectItem item : node.getSelectItems()) {
            process(item, context);
        }

        return null;
    }

    @Override
    protected Symbol visitSingleColumn(SingleColumn node, Analysis context) {
        Symbol symbol = process(node.getExpression(), context);
        context.outputSymbols().add(symbol);

        if (node.getAlias().isPresent()) {
            context.addAlias(node.getAlias().get(), symbol);
        } else {
            context.addAlias(outputNameFormatter.process(node.getExpression(), null), symbol);
        }

        return null;
    }

    @Override
    protected Symbol visitAllColumns(AllColumns node, Analysis context) {
        Symbol symbol;
        for (ReferenceInfo referenceInfo : context.table().columns()) {
            symbol = context.allocateReference(referenceInfo.ident());
            context.outputSymbols().add(symbol);
            context.addAlias(referenceInfo.ident().columnIdent().name(), symbol);
        }

        return null;
    }

    @Override
    protected Symbol visitTable(Table node, Analysis context) {
        Preconditions.checkState(context.table() == null, "selecting from multiple tables is not supported");
        context.table(TableIdent.of(node));
        return null;
    }


    @Override
    protected Symbol visitFunctionCall(FunctionCall node, Analysis context) {
        List<Symbol> arguments = new ArrayList<>(node.getArguments().size());
        List<DataType> argumentTypes = new ArrayList<>(node.getArguments().size());
        for (Expression expression : node.getArguments()) {
            ValueSymbol vs = (ValueSymbol) expression.accept(this, context);
            arguments.add(vs);
            argumentTypes.add(vs.valueType());
        }
        FunctionIdent ident = new FunctionIdent(node.getName().toString(), argumentTypes);
        FunctionInfo functionInfo = context.getFunctionInfo(ident);

        return context.allocateFunction(functionInfo, arguments);
    }

    @Override
    protected Symbol visitInPredicate(InPredicate node, Analysis context) {
        List<Symbol> arguments = new ArrayList<>(2);
        List<DataType> argumentTypes = new ArrayList<>(2);

        Symbol value = process(node.getValue(), context);

        arguments.add(value);
        arguments.add(process(node.getValueList(), context));

        DataType valueDataType = symbolDataTypeVisitor.process(value, context);
        argumentTypes.add(valueDataType);
        argumentTypes.add(DataType.SET_TYPES.get(valueDataType.ordinal()));

        FunctionIdent functionIdent = new FunctionIdent(InOperator.NAME, argumentTypes);
        FunctionInfo functionInfo = context.getFunctionInfo(functionIdent);
        return context.allocateFunction(functionInfo, arguments);
    }

    @Override
    protected Symbol visitInListExpression(InListExpression node, Analysis context) {
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

        dataType = DataType.SET_TYPES.get(dataType.ordinal());
        return new SetLiteral(dataType, symbols);
    }

    @Override
    protected Symbol visitStringLiteral(StringLiteral node, Analysis context) {
        return new io.crate.planner.symbol.StringLiteral(node.getValue());
    }

    @Override
    protected Symbol visitDoubleLiteral(DoubleLiteral node, Analysis context) {
        return new io.crate.planner.symbol.DoubleLiteral(node.getValue());
    }

    @Override
    protected Symbol visitLongLiteral(LongLiteral node, Analysis context) {
        return new io.crate.planner.symbol.LongLiteral(node.getValue());
    }

    @Override
    public Symbol visitParameterExpression(ParameterExpression node, Analysis context) {
        Object parameter = context.parameterAt(node.position());
        DataType type = DataType.forClass(parameter.getClass());
        if (type == null) {
            throw new UnsupportedOperationException("Unsupported parameter type " + parameter.getClass());
        }
        return io.crate.planner.symbol.Literal.forType(type, parameter);
    }

    @Override
    protected Symbol visitQualifiedNameReference(QualifiedNameReference node, Analysis context) {
        Symbol symbol = context.symbolFromAlias(node.getSuffix().getSuffix());
        if (symbol != null) {
            return symbol;
        }
        return context.allocateReference(new ReferenceIdent(context.table().ident(), node.getSuffix().getSuffix()));
    }

    @Override
    protected Symbol visitSubscriptExpression(SubscriptExpression node, Analysis context) {
        SubscriptContext subscriptContext = new SubscriptContext();
        node.accept(visitor, subscriptContext);
        ReferenceIdent ident = new ReferenceIdent(
                context.table().ident(), subscriptContext.column(), subscriptContext.parts());
        return context.allocateReference(ident);
    }

    @Override
    protected Symbol visitLogicalBinaryExpression(LogicalBinaryExpression node, Analysis context) {
        List<Symbol> arguments = new ArrayList<>(2);
        arguments.add(process(node.getLeft(), context));
        arguments.add(process(node.getRight(), context));

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

        return context.allocateFunction(functionInfo, arguments);
    }

    @Override
    protected Symbol visitComparisonExpression(ComparisonExpression node, Analysis context) {
        String operatorName = "op_" + node.getType().getValue();

        // resolve arguments
        List<Symbol> arguments = new ArrayList<>(2);
        arguments.add(process(node.getLeft(), context));
        arguments.add(process(node.getRight(), context));

        // resolve argument types
        List<DataType> argumentTypes = new ArrayList<>(arguments.size());
        argumentTypes.add(symbolDataTypeVisitor.process(arguments.get(0), context));
        argumentTypes.add(symbolDataTypeVisitor.process(arguments.get(1), context));

        // swap statements like  eq(2, name) to eq(name, 2)
        if (arguments.get(0).symbolType().isLiteral() && arguments.get(1).symbolType() == SymbolType.REFERENCE) {
            if (swapOperatorTable.containsKey(operatorName)) {
                operatorName = swapOperatorTable.get(operatorName);
            }
            Collections.reverse(arguments);
            Collections.reverse(argumentTypes);
        }

        // try implicit type cast (conversion)
        if (argumentTypes.get(0) != argumentTypes.get(1)) {
            Symbol convertedSymbol = ((io.crate.planner.symbol.Literal)arguments.get(1)).convertTo(argumentTypes.get(0));
            arguments.set(1, convertedSymbol);
            argumentTypes.set(1, argumentTypes.get(0));
        }

        FunctionIdent functionIdent = new FunctionIdent(operatorName, argumentTypes);
        FunctionInfo functionInfo = context.getFunctionInfo(functionIdent);

        return context.allocateFunction(functionInfo, arguments);
    }

}
