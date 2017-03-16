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

package io.crate.operation.udf;

import io.crate.analyze.CreateFunctionAnalyzedStatement;
import io.crate.analyze.DropFunctionAnalyzedStatement;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.data.Row;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.concurrent.CompletableFuture;

@Singleton
public class UserDefinedFunctionDDLDispatcher {

    private final TransportCreateUserDefinedFunctionAction createUserDefinedFunctionAction;

    @Inject
    public UserDefinedFunctionDDLDispatcher(TransportCreateUserDefinedFunctionAction createUserDefinedFunctionAction) {
        this.createUserDefinedFunctionAction = createUserDefinedFunctionAction;
    }

    public CompletableFuture<Long> dispatch(final CreateFunctionAnalyzedStatement statement, Row params) {
        final CompletableFuture<Long> resultFuture = new CompletableFuture<>();
        UserDefinedFunctionMetaData metaData = new UserDefinedFunctionMetaData(
            statement.schema(),
            statement.name(),
            statement.arguments(),
            statement.returnType(),
            ExpressionToStringVisitor.convert(statement.language(), params),
            ExpressionToStringVisitor.convert(statement.definition(), params)
        );
        CreateUserDefinedFunctionRequest request = new CreateUserDefinedFunctionRequest(metaData, statement.replace());
        createUserDefinedFunctionAction.execute(
            request,
            new ActionListener<TransportUserDefinedFunctionResponse>() {
                @Override
                public void onResponse(TransportUserDefinedFunctionResponse transportUserDefinedFunctionResponse) {
                    resultFuture.complete(1L);
                }

                @Override
                public void onFailure(Throwable e) {
                    resultFuture.completeExceptionally(e);
                }
            }
        );

        return resultFuture;
    }

    public CompletableFuture<Long> dispatch(final DropFunctionAnalyzedStatement statement, Row params) {
        final CompletableFuture<Long> resultFuture = new CompletableFuture<>();
        DropUserDefinedFunctionRequest request = new DropUserDefinedFunctionRequest( // TODO: add schema
            UserDefinedFunctionMetaData.createMethodSignature(null, statement.name(), statement.arguments()),
            statement.ifExists()
        );
        transportActionProvider.transportDropUserDefinedFunctionAction().execute(
            request,
            new ActionListener<TransportUserDefinedFunctionResponse>() {
                @Override
                public void onResponse(TransportUserDefinedFunctionResponse transportUserDefinedFunctionResponse) {
                    resultFuture.complete(1L);
                }

                @Override
                public void onFailure(Throwable e) {
                    resultFuture.completeExceptionally(e);
                }
            }
        );
        return resultFuture;
    }
}
