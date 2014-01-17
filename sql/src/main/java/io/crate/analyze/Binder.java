package io.crate.analyze;

import io.crate.metadata.*;
import io.crate.sql.parser.StatementBuilder;
import io.crate.sql.tree.*;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.Inject;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.List;

public class Binder {

    private final ReferenceResolver referenceResolver;
    private final TableBindingVisitor tableBinder = new TableBindingVisitor();
    private final DataTypeGatherer dataTypeGatherer = new DataTypeGatherer();
    private final BindingRewritingTraversal rewritingVisitor = new BindingRewritingTraversal(new BindingRewritingVisitor());
    private final Functions functions;

    @Inject
    public Binder(ReferenceResolver referenceResolver, Functions functions) {
        this.referenceResolver = referenceResolver;
        this.functions = functions;
    }

    public void bind(Statement statement) {
        BindingContext context = new BindingContext(referenceResolver, functions);

        statement.accept(tableBinder, context);
        statement.accept(dataTypeGatherer, context);
        statement.accept(rewritingVisitor, context);
    }

    static class TableBindingVisitor extends DefaultTraversalVisitor<Object, BindingContext> {

        @Override
        protected Object visitTable(Table node, BindingContext context) {
            assert context.table() == null: "selecting from multiple tables is not supported";
            context.table(TableIdent.of(node));
            return null;
        }
    }

    static class DataTypeGatherer extends DefaultTraversalVisitor<DataType, BindingContext> {

        SubscriptVisitor visitor = new SubscriptVisitor();

        @Override
        protected DataType visitFunctionCall(FunctionCall node, BindingContext context) {
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
        protected DataType visitStringLiteral(StringLiteral node, BindingContext context) {
            return DataType.STRING;
        }

        // TODO: implement for every expression that can be used as an argument to a functionCall

        @Override
        protected DataType visitQualifiedNameReference(QualifiedNameReference node, BindingContext context) {
            ReferenceInfo info = context.getReferenceInfo(
                    new ReferenceIdent(context.table(), node.getSuffix().getSuffix())
            );
            context.putReferenceInfo(node, info);
            return info.type();
        }

        @Override
        protected DataType visitSubscriptExpression(SubscriptExpression node, BindingContext context) {
            SubscriptContext subscriptContext = new SubscriptContext();
            node.accept(visitor, subscriptContext);
            ReferenceIdent ident = new ReferenceIdent(
                    context.table(), subscriptContext.column(), subscriptContext.parts());
            ReferenceInfo info = context.getReferenceInfo(ident);
            context.putReferenceInfo(node, info);
            return info.type();
        }
    }
}
