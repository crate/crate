package io.crate.analyze;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.TableIdent;
import io.crate.operator.operator.*;
import io.crate.planner.symbol.*;
import io.crate.sql.tree.*;
import io.crate.sql.tree.DoubleLiteral;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.StringLiteral;
import org.cratedb.DataType;

import java.util.*;

class StatementAnalyzer extends DefaultTraversalVisitor<Symbol, Analysis> {

    protected static SubscriptVisitor visitor = new SubscriptVisitor();
    protected static SymbolDataTypeVisitor symbolDataTypeVisitor = new SymbolDataTypeVisitor();
    private final Map<String, String> swapOperatorTable = ImmutableMap.<String, String>builder()
            .put(GtOperator.NAME, LtOperator.NAME)
            .put(GteOperator.NAME, LteOperator.NAME)
            .put(LtOperator.NAME, GtOperator.NAME)
            .put(LteOperator.NAME, GteOperator.NAME)
            .build();


    @Override
    protected Symbol visitQuerySpecification(QuerySpecification node, Analysis context) {
        // visit the from first, since this qualifies the select
        if (node.getFrom() != null) {
            for (Relation relation : node.getFrom()) {
                process(relation, context);
            }
        }

        if (node.getLimit().isPresent()) {
            context.limit(Integer.parseInt(node.getLimit().get()));
        }

        process(node.getSelect(), context);
        if (node.getWhere().isPresent()) {
            Function function = (Function) process(node.getWhere().get(), context);
            context.whereClause(function);
        }


        if (node.getGroupBy().size() > 0) {
            List<Symbol> groupBy = new ArrayList<>(node.getGroupBy().size());
            for (Expression expression : node.getGroupBy()) {
                Symbol s = process(expression, context);
                // TODO: support column names and ordinals
                int idx = context.outputSymbols().indexOf(s);
                Preconditions.checkArgument(idx >= 0,
                        "group by expression is not in output columns", s);
                groupBy.add(context.outputSymbols().get(idx));
            }
            context.groupBy(groupBy);
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
        // TODO: support offset, needs parser impl
        return null;
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
        List<Symbol> outputSymbols = new ArrayList<>(node.getSelectItems().size());

        // TODO: better output names
        context.outputNames(new ArrayList<String>(node.getSelectItems().size()));
        for (SelectItem item : node.getSelectItems()) {
            outputSymbols.add(process(item, context));
        }
        context.outputSymbols(outputSymbols);
        return null;
    }

    @Override
    protected Symbol visitSingleColumn(SingleColumn node, Analysis context) {
        context.addOutputName(node.toString());
        return process(node.getExpression(), context);
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
        List<DataType> argumentTypes = new ArrayList<>(arguments.size());

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
        Set<io.crate.planner.symbol.Literal> symbols = new HashSet<>();
        DataType dataType = null;
        for (Expression expression : node.getValues()) {
            Symbol s = process(expression, context);
            if (s.symbolType().isLiteral()) {
                io.crate.planner.symbol.Literal l = (io.crate.planner.symbol.Literal) s;
                if (dataType == null) {
                    dataType = DataType.SET_TYPES.get(l.valueType().ordinal());
                } else if (dataType != DataType.SET_TYPES.get(l.valueType().ordinal())) {
                    throw new IllegalArgumentException("Data types do not match.");
                }
                symbols.add(l);
            } else {
                return s;
            }
        }

        return SetLiteral.forType(dataType, symbols);
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

    // TODO: implement for every expression that can be used as an argument to a functionCall

    @Override
    protected Symbol visitQualifiedNameReference(QualifiedNameReference node, Analysis context) {
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
