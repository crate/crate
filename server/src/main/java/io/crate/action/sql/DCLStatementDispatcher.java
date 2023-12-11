/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.action.sql;

import io.crate.analyze.AnalyzedPrivileges;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.role.RoleManager;
import io.crate.data.Row;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;

import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * Visitor that dispatches requests based on Analysis class to different DCL actions.
 * <p>
 * Its methods return a future returning a Long containing the response rowCount.
 */
@Singleton
public class DCLStatementDispatcher implements BiFunction<AnalyzedStatement, Row, CompletableFuture<Long>> {

    private final InnerVisitor INNER_VISITOR = new InnerVisitor();
    private final RoleManager roleManager;

    @Inject
    public DCLStatementDispatcher(Provider<RoleManager> userManagerProvider) {
        this.roleManager = userManagerProvider.get();
    }

    @Override
    public CompletableFuture<Long> apply(AnalyzedStatement analyzedStatement, Row row) {
        return analyzedStatement.accept(INNER_VISITOR, roleManager);
    }

    private static class InnerVisitor extends AnalyzedStatementVisitor<RoleManager, CompletableFuture<Long>> {

        @Override
        protected CompletableFuture<Long> visitAnalyzedStatement(AnalyzedStatement analyzedStatement, RoleManager roleManager) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException(String.format(Locale.ENGLISH, "Can't handle \"%s\"", analyzedStatement)));
        }

        @Override
        public CompletableFuture<Long> visitPrivilegesStatement(AnalyzedPrivileges analysis, RoleManager roleManager) {
            return roleManager.applyPrivileges(analysis.userNames(), analysis.privileges());
        }
    }
}
