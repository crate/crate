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

package io.crate.planner.optimizer;

import io.crate.action.sql.SessionContext;
import io.crate.user.User;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.SearchPath;
import io.crate.metadata.settings.SessionSettings;
import io.crate.planner.optimizer.rule.MergeFilters;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static io.crate.analyze.SymbolEvaluator.evaluateWithoutParams;
import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class OptimizerRuleSessionSettingProviderTest {

    private NodeContext nodeCtx = createNodeContext();

    private Function<Symbol, Object> eval = x -> evaluateWithoutParams(
        CoordinatorTxnCtx.systemTransactionContext(),
        nodeCtx,
        x
    );

    @Test
    public void test_optimizer_rule_session_settings() {
        var settingsProvider = new LoadedRules();
        var sessionSetting = settingsProvider.buildRuleSessionSetting(MergeFilters.class);

        assertThat(sessionSetting.name(), is("optimizer_merge_filters"));
        assertThat(sessionSetting.description(), is("Indicates if the optimizer rule MergeFilters is activated."));
        assertThat(sessionSetting.defaultValue(), is("true"));

        var mergefilterSettings = new SessionSettings("user",
                                                      SearchPath.createSearchPathFrom("dummySchema"),
                                                      true,
                                                      Set.of(MergeFilters.class),
                                                      true);

        assertThat(sessionSetting.getValue(mergefilterSettings), is("false"));

        var sessionContext = new SessionContext(User.of("user"));

        // Disable MergeFilters 'SET SESSION optimizer_merge_filters = false'
        sessionSetting.apply(sessionContext, List.of(Literal.of(false)), eval);
        assertThat(sessionContext.excludedOptimizerRules(), containsInAnyOrder(MergeFilters.class));

        // Enable MergeFilters 'SET SESSION optimizer_merge_filters = true'
        sessionSetting.apply(sessionContext, List.of(Literal.of(true)), eval);
        assertThat(sessionContext.excludedOptimizerRules().isEmpty(), is(true));
    }
}
