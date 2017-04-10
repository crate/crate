/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.settings.session;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.expressions.ExpressionToStringVisitor;

import java.util.Map;

public class SessionSettingRegistry {

    private static final Map<String, SessionSettingApplier> SESSION_SETTINGS =
        ImmutableMap.<String, SessionSettingApplier>builder()
            .put("search_path", (parameters, expressions, context) -> {
                if (expressions.size() > 0) {
                    // The search_path takes a schema name as a string or comma-separated list of schema names.
                    // In the second case only the first schema in the list will be used.
                    // Resetting the search path with `set search_path to default` results
                    // in the empty list of expressions.
                    String schema = ExpressionToStringVisitor.convert(expressions.get(0), parameters);
                    context.setDefaultSchema(schema.trim());
                } else {
                    context.setDefaultSchema(null);
                }
            }).build();


    public static SessionSettingApplier getApplier(String setting) {
        return SESSION_SETTINGS.get(setting);
    }
}
