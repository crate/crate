package io.crate.analyze;

import com.google.common.base.Preconditions;
import io.crate.metadata.*;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.ValueSymbol;
import io.crate.sql.tree.*;
import org.cratedb.DataType;

import java.util.ArrayList;
import java.util.List;

class StatementAnalyzer extends DefaultTraversalVisitor<Symbol, Analysis> {

    SubscriptVisitor visitor = new SubscriptVisitor();

    @Override
    protected Symbol visitQuerySpecification(QuerySpecification node, Analysis context) {
        // visit the from first, since this qualifies the select
        if (node.getFrom() != null) {
            for (Relation relation : node.getFrom()) {
                process(relation, context);
            }
        }

        if (node.getLimit().isPresent()){
            context.limit(Integer.parseInt(node.getLimit().get()));
        }

        process(node.getSelect(), context);
        if (node.getWhere().isPresent()) {
            process(node.getWhere().get(), context);
        }

        if (node.getGroupBy().size()>0){
            List<Symbol> groupBy = new ArrayList<>(node.getGroupBy().size());
            for (Expression expression : node.getGroupBy()) {
                Symbol s = process(expression, context);
                // TODO: support column names and ordinals
                int idx = context.outputSymbols().indexOf(s);
                Preconditions.checkArgument(idx>=0,
                        "group by expression is not in output columns", s);
                groupBy.add(context.outputSymbols().get(idx));
            }
            context.groupBy(groupBy);
        }

        Preconditions.checkArgument(node.getHaving().isPresent()==false, "having clause is not yet supported");

        if (node.getOrderBy().size()>0){
            List<Symbol> sortSymbols = new ArrayList<>(node.getOrderBy().size());
            context.reverseFlags(new ArrayList<Boolean>(sortSymbols.size()));
            for (SortItem sortItem : node.getOrderBy()) {
                sortSymbols.add(process(sortItem, context));
            }
            context.sortSymbols(sortSymbols);
        }
        // TODO: support offset, needs parser impl
        return null;
    }

    @Override
    protected Symbol visitSortItem(SortItem node, Analysis context) {
        context.reverseFlags().add(node.getOrdering()== SortItem.Ordering.DESCENDING);
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
        List<ValueSymbol> arguments = new ArrayList<>(node.getArguments().size());
        List<DataType> argumentTypes = new ArrayList<>(node.getArguments().size());
        for (Expression expression : node.getArguments()) {
            ValueSymbol vs = (ValueSymbol) expression.accept(this, context);
            arguments.add(vs);
            argumentTypes.add(vs.valueType());
        }
        FunctionIdent ident = new FunctionIdent(node.getName().toString(), argumentTypes);
        FunctionInfo functionInfo = context.getFunctionInfo(ident);

        Function function = new Function(functionInfo, arguments);
        return function;
    }

    @Override
    protected Symbol visitStringLiteral(StringLiteral node, Analysis context) {
        context.putType(node, DataType.STRING);
        return new io.crate.planner.symbol.StringLiteral(node.getValue());
    }

    // TODO: implement for every expression that can be used as an argument to a functionCall

    @Override
    protected Symbol visitQualifiedNameReference(QualifiedNameReference node, Analysis context) {
        ReferenceInfo info = context.getReferenceInfo(
                new ReferenceIdent(context.table(), node.getSuffix().getSuffix())
        );
        //context.putReferenceInfo(node, info);
        return new Reference(info);
    }

    @Override
    protected Symbol visitSubscriptExpression(SubscriptExpression node, Analysis context) {
        SubscriptContext subscriptContext = new SubscriptContext();
        node.accept(visitor, subscriptContext);
        ReferenceIdent ident = new ReferenceIdent(
                context.table(), subscriptContext.column(), subscriptContext.parts());
        ReferenceInfo info = context.getReferenceInfo(ident);
        //context.putReferenceInfo(node, info);
        return new Reference(info);
    }
}
