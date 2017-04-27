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

package io.crate.plugin;

import io.crate.action.sql.DDLStatementDispatcher;
import io.crate.action.sql.SQLOperations;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.operation.auth.AuthenticationProvider;
import io.crate.operation.udf.TransportCreateUserDefinedFunctionAction;
import io.crate.operation.udf.TransportDropUserDefinedFunctionAction;
import io.crate.operation.udf.UserDefinedFunctionService;
import io.crate.operation.user.UserManagerProvider;
import io.crate.planner.Planner;
import io.crate.planner.TableStats;
import io.crate.planner.TableStatsService;
import io.crate.protocols.postgres.PostgresNetty;
import org.elasticsearch.common.inject.AbstractModule;


public class SQLModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(DDLStatementDispatcher.class).asEagerSingleton();
        bind(FulltextAnalyzerResolver.class).asEagerSingleton();
        bind(PostgresNetty.class).asEagerSingleton();
        bind(SQLOperations.class).asEagerSingleton();
        bind(Planner.class).asEagerSingleton();
        bind(TableStats.class).asEagerSingleton();
        bind(TableStatsService.class).asEagerSingleton();
        bind(UserDefinedFunctionService.class).asEagerSingleton();
        bind(TransportCreateUserDefinedFunctionAction.class).asEagerSingleton();
        bind(TransportDropUserDefinedFunctionAction.class).asEagerSingleton();
        bind(AuthenticationProvider.class).asEagerSingleton();
        bind(UserManagerProvider.class).asEagerSingleton();
    }
}
