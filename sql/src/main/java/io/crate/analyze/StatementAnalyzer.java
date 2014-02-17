package io.crate.analyze;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.TableIdent;
import io.crate.operator.aggregation.impl.CollectSetAggregation;
import io.crate.operator.operator.*;
import io.crate.planner.symbol.*;
import io.crate.planner.symbol.Literal;
import io.crate.sql.ExpressionFormatter;
import io.crate.sql.tree.BooleanLiteral;
import io.crate.sql.tree.*;
import io.crate.sql.tree.DoubleLiteral;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.StringLiteral;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;

import java.util.*;

abstract class StatementAnalyzer<T extends Analysis> extends DefaultTraversalVisitor<Symbol, T> {

    protected static OutputNameFormatter outputNameFormatter = new OutputNameFormatter();

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
    protected Symbol visitNode(Node node, T context) {
        System.out.println("Not analyzed node: " + node);
        return super.visitNode(node, context);
    }

    @Override
    protected Symbol visitTable(Table node, T context) {
        Preconditions.checkState(context.table() == null, "selecting from multiple tables is not supported");
        context.table(TableIdent.of(node));
        return null;
    }


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
            ImmutableList<Symbol> outerArguments = ImmutableList.<Symbol>of(innerFunction);
            ImmutableList<DataType> outerArgumentTypes = ImmutableList.of(DataType.SET_TYPES.get(argumentTypes.get(0).ordinal()));

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
    protected Symbol visitBooleanLiteral(BooleanLiteral node, Analysis context) {
        return new io.crate.planner.symbol.BooleanLiteral(node.getValue());
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
    public Symbol visitParameterExpression(ParameterExpression node, T context) {
        Object parameter = context.parameterAt(node.position());
        DataType type = DataType.forClass(parameter.getClass());
        if (type == null) {
            throw new UnsupportedOperationException("Unsupported parameter type " + parameter.getClass());
        }
        return io.crate.planner.symbol.Literal.forType(type, parameter);
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
    protected Symbol visitComparisonExpression(ComparisonExpression node, T context) {
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
            Symbol convertedSymbol = ((io.crate.planner.symbol.Literal) arguments.get(1)).convertTo(argumentTypes.get(0));
            arguments.set(1, convertedSymbol);
            argumentTypes.set(1, argumentTypes.get(0));
        }

        FunctionIdent functionIdent = new FunctionIdent(operatorName, argumentTypes);
        FunctionInfo functionInfo = context.getFunctionInfo(functionIdent);
        return context.allocateFunction(functionInfo, arguments);
    }

    @Override
    public Symbol visitDelete(Delete node, T context) {
        context.isDelete(true);

        process(node.getTable(), context);

        if (node.getWhere().isPresent()) {
            Function function = (Function) process(node.getWhere().get(), context);
            context.whereClause(function);
        }

        return null;
    }

}
