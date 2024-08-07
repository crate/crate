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

package io.crate.expression.predicate;

import org.elasticsearch.common.settings.Settings;

import io.crate.expression.operator.DistinctFromPredicate;
import io.crate.metadata.Functions.Builder;
import io.crate.metadata.FunctionsProvider;
import io.crate.metadata.settings.session.SessionSettingRegistry;

public class Predicates implements FunctionsProvider {

    @Override
    public void addFunctions(Settings settings,
                             SessionSettingRegistry sessionSettingRegistry,
                             Builder builder) {
        IsNullPredicate.register(builder);
        NotPredicate.register(builder);
        MatchPredicate.register(builder);
        DistinctFromPredicate.register(builder);
    }
}
