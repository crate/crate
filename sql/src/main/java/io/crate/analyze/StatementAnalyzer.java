package io.crate.analyze;

import com.google.common.base.Preconditions;
import io.crate.metadata.*;
import io.crate.sql.tree.*;
import org.cratedb.DataType;

import java.util.ArrayList;
import java.util.List;

class StatementAnalyzer extends DefaultTraversalVisitor<DataType, Analysis> {

    SubscriptVisitor visitor = new SubscriptVisitor();

    @Override
    protected DataType visitQuery(Query node, Analysis context) {
        context.query(node);
        return super.visitQuery(node, context);
    }

    @Override
    protected DataType visitTable(Table node, Analysis context) {
        Preconditions.checkState(context.table() == null, "selecting from multiple tables is not supported");
        context.table(TableIdent.of(node));
        return null;
    }


    @Override
    protected DataType visitFunctionCall(FunctionCall node, Analysis context) {
        List<DataType> argumentTypes = new ArrayList<>(node.getArguments().size());

        for (Expression expression : node.getArguments()) {
            argumentTypes.add(expression.accept(this, context));
        }

        FunctionIdent ident = new FunctionIdent(node.getName().toString(), argumentTypes);
        FunctionInfo functionInfo = context.getFunctionInfo(ident);
        DataType dataType = functionInfo.returnType();
        context.putFunctionInfo(node, functionInfo);

        return dataType;
    }

    @Override
    protected DataType visitStringLiteral(StringLiteral node, Analysis context) {
        context.putType(node, DataType.STRING);
        return DataType.STRING;
    }

    // TODO: implement for every expression that can be used as an argument to a functionCall

    @Override
    protected DataType visitQualifiedNameReference(QualifiedNameReference node, Analysis context) {
        ReferenceInfo info = context.getReferenceInfo(
                new ReferenceIdent(context.table(), node.getSuffix().getSuffix())
        );
        context.putReferenceInfo(node, info);
        return info.type();
    }

    @Override
    protected DataType visitSubscriptExpression(SubscriptExpression node, Analysis context) {
        SubscriptContext subscriptContext = new SubscriptContext();
        node.accept(visitor, subscriptContext);
        ReferenceIdent ident = new ReferenceIdent(
                context.table(), subscriptContext.column(), subscriptContext.parts());
        ReferenceInfo info = context.getReferenceInfo(ident);
        context.putReferenceInfo(node, info);
        return info.type();
    }
}
