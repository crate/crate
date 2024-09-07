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

package io.crate.expression.operator;

import java.util.Set;

import org.elasticsearch.common.settings.Settings;

import io.crate.expression.operator.all.AllOperator;
import io.crate.expression.operator.any.AnyOperator;
import io.crate.expression.predicate.NotPredicate;
import io.crate.metadata.Functions;
import io.crate.metadata.FunctionsProvider;
import io.crate.metadata.settings.session.SessionSettingRegistry;

public class Operators implements FunctionsProvider {

    public static final Set<String> LOGICAL_OPERATORS = Set.of(
        AndOperator.NAME, OrOperator.NAME, NotPredicate.NAME
    );

    public static final Set<String> COMPARISON_OPERATORS = Set.of(
        EqOperator.NAME,
        GtOperator.NAME, GteOperator.NAME,
        LtOperator.NAME, LteOperator.NAME,
        CIDROperator.CONTAINED_WITHIN
    );

    @Override
    public void addFunctions(Settings settings,
                             SessionSettingRegistry sessionSettingRegistry,
                             Functions.Builder builder) {
        AndOperator.register(builder);
        OrOperator.register(builder);
        EqOperator.register(builder);
        CIDROperator.register(builder);
        LtOperator.register(builder);
        LteOperator.register(builder);
        GtOperator.register(builder);
        GteOperator.register(builder);
        RegexpMatchOperator.register(builder);
        RegexpMatchCaseInsensitiveOperator.register(builder);

        AnyOperator.register(builder);
        AllOperator.register(builder);
        LikeOperators.register(builder);
        ExistsOperator.register(builder);
    }
}
