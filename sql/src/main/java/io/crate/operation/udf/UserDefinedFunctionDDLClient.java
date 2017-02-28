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

import io.crate.action.FutureActionListener;
import io.crate.analyze.CreateFunctionAnalyzedStatement;
import io.crate.analyze.DropFunctionAnalyzedStatement;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.data.Row;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.concurrent.CompletableFuture;

@Singleton
public class UserDefinedFunctionDDLClient {

    private final TransportCreateUserDefinedFunctionAction createUserDefinedFunctionAction;
    private final TransportDropUserDefinedFunctionAction dropUserDefinedFunctionAction;

    @Inject
    public UserDefinedFunctionDDLClient(TransportCreateUserDefinedFunctionAction createUserDefinedFunctionAction,
                                        TransportDropUserDefinedFunctionAction dropUserDefinedFunctionAction) {
        this.createUserDefinedFunctionAction = createUserDefinedFunctionAction;
        this.dropUserDefinedFunctionAction = dropUserDefinedFunctionAction;
    }

    public CompletableFuture<Long> execute(final CreateFunctionAnalyzedStatement statement, Row params) {
        UserDefinedFunctionMetaData metaData = new UserDefinedFunctionMetaData(
            statement.schema(),
            statement.name(),
            statement.arguments(),
            statement.returnType(),
            ExpressionToStringVisitor.convert(statement.language(), params),
            ExpressionToStringVisitor.convert(statement.definition(), params)
        );
        CreateUserDefinedFunctionRequest request = new CreateUserDefinedFunctionRequest(metaData, statement.replace());
        FutureActionListener<UserDefinedFunctionResponse, Long> listener = new FutureActionListener<>(r -> 1L);
        createUserDefinedFunctionAction.execute(request, listener);
        return listener;
    }

    public CompletableFuture<Long> execute(final DropFunctionAnalyzedStatement statement) {
        DropUserDefinedFunctionRequest request = new DropUserDefinedFunctionRequest(
            statement.schema(),
            statement.name(),
            statement.argumentTypes(),
            statement.ifExists()
        );
        FutureActionListener<UserDefinedFunctionResponse, Long> listener = new FutureActionListener<>(r -> 1L);
        dropUserDefinedFunctionAction.execute(request, listener);
        return listener;
    }
}
