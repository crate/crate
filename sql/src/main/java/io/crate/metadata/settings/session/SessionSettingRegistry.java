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
import io.crate.analyze.expressions.ExpressionToObjectVisitor;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.sql.tree.Expression;
import io.crate.types.BooleanType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Map;

public class SessionSettingRegistry {

    public static final String SEARCH_PATH_KEY = "search_path";
    public static final String SEMI_JOIN_KEY = "enable_semijoin";
    public static final String HASH_JOIN_KEY = "enable_hashjoin";

    private static final Map<String, SessionSettingApplier> SESSION_SETTINGS =
        ImmutableMap.<String, SessionSettingApplier>builder()
            .put(SEARCH_PATH_KEY, (parameters, expressions, context) -> {
                if (expressions.size() > 0) {
                    // Resetting the search path with `set search_path to default` results
                    // in the empty list of expressions.
                    String[] schemas = new String[expressions.size()];
                    for (int i = 0; i < expressions.size(); i++) {
                        Expression expression = expressions.get(i);
                        schemas[i] = ExpressionToStringVisitor.convert(expression, parameters).trim();
                    }
                    context.setSearchPath(schemas);
                } else {
                    context.resetSchema();
                }
            })
            .put(SEMI_JOIN_KEY, (parameters, expressions, context) -> {
                if (expressions.size() == 1) {
                    Object value = ExpressionToObjectVisitor.convert(expressions.get(0), parameters);
                    boolean booleanValue = BooleanType.INSTANCE.value(value);
                    context.setSemiJoinsRewriteEnabled(booleanValue);
                } else {
                    throw new IllegalArgumentException(SEMI_JOIN_KEY + " should have only one argument.");
                }
            })
            .put(HASH_JOIN_KEY, (parameters, expressions, context) -> {
                if (expressions.size() == 1) {
                    Object value = ExpressionToObjectVisitor.convert(expressions.get(0), parameters);
                    boolean booleanValue = BooleanType.INSTANCE.value(value);
                    context.setHashJoinEnabled(booleanValue);
                } else {
                    throw new IllegalArgumentException(HASH_JOIN_KEY + " should have only one argument.");
                }
            })
            .build();


    public static SessionSettingApplier getApplier(String setting) {
        return SESSION_SETTINGS.get(setting);
    }

    public static DataType dataTypeOfParameter(String parameterName) {
        switch (parameterName) {
            case SEARCH_PATH_KEY:
                return DataTypes.STRING_ARRAY;
            case SEMI_JOIN_KEY:
                return DataTypes.BOOLEAN;
            case HASH_JOIN_KEY:
                return DataTypes.BOOLEAN;
            default:
                throw new IllegalArgumentException("Cannot resolve data type of parameter '" + parameterName + "'");
        }
    }
}
