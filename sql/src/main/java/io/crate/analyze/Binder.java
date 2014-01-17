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
