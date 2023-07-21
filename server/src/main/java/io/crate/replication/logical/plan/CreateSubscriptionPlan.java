/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.replication.logical.plan;


import java.util.Locale;
import java.util.function.Function;

import org.elasticsearch.common.settings.Settings;

import io.crate.analyze.SymbolEvaluator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.replication.logical.action.CreateSubscriptionRequest;
import io.crate.replication.logical.analyze.AnalyzedCreateSubscription;
import io.crate.replication.logical.exceptions.CreateSubscriptionException;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.types.DataTypes;

public class CreateSubscriptionPlan implements Plan {

    private final AnalyzedCreateSubscription analyzedCreateSubscription;

    public CreateSubscriptionPlan(AnalyzedCreateSubscription analyzedCreateSubscription) {
        this.analyzedCreateSubscription = analyzedCreateSubscription;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) throws Exception {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            x,
            params,
            subQueryResults
        );

        var url = validateAndConvertToString(eval.apply(analyzedCreateSubscription.connectionInfo()));
        var connectionInfo = ConnectionInfo.fromURL(url);
        var settings = Settings.builder().put(analyzedCreateSubscription.properties().map(eval)).build();

        var subscribingUser = connectionInfo.settings().get(ConnectionInfo.USERNAME.getKey());
        if (subscribingUser == null || subscribingUser.isEmpty()) {
            throw new CreateSubscriptionException(
                String.format(Locale.ENGLISH, "Setting '%s' must be provided on CREATE SUBSCRIPTION", ConnectionInfo.USERNAME.getKey())
            );
        }

        if (settings.names().isEmpty() == false) {
            throw new CreateSubscriptionException("Settings with 'WITH' clause are not supported for CREATE SUBSCRIPTION");
        }

        var request = new CreateSubscriptionRequest(
            plannerContext.transactionContext().sessionSettings().sessionUser().name(),
            analyzedCreateSubscription.name(),
            connectionInfo,
            analyzedCreateSubscription.publications(),
            settings
        );

        dependencies.createSubscriptionAction().execute(request)
            .whenComplete(new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1 : 1L)));
    }

    private static String validateAndConvertToString(Object uri) {
        if (uri instanceof String str) {
            return str;
        }
        throw new CreateSubscriptionException("fileUri must be of type STRING. Got " + DataTypes.guessType(uri));
    }
}
