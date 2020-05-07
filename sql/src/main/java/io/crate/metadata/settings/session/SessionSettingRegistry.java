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

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;
import io.crate.metadata.SearchPath;
import io.crate.planner.optimizer.Rule;
import io.crate.protocols.postgres.PostgresWireProtocol;
import io.crate.types.DataTypes;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

import static io.crate.metadata.SearchPath.createSearchPathFrom;

public class SessionSettingRegistry {

    private static final String SEARCH_PATH_KEY = "search_path";
    public static final String HASH_JOIN_KEY = "enable_hashjoin";
    static final String MAX_INDEX_KEYS = "max_index_keys";
    private static final String SERVER_VERSION_NUM = "server_version_num";
    private static final String SERVER_VERSION = "server_version";
    private static final String OPTIMIZER_RULE = "optimizer_";

    public static final Map<String, SessionSetting<?>> SETTINGS = ImmutableMap.<String, SessionSetting<?>>builder()
            .put(SEARCH_PATH_KEY,
                new SessionSetting<>(
                    objects -> {}, // everything allowed, empty list (resulting by ``SET .. TO DEFAULT`` results in defaults
                    objects -> createSearchPathFrom(objectsToStringArray(objects)),
                    SessionContext::setSearchPath,
                    s -> iterableToString(s.searchPath()),
                    () -> iterableToString(SearchPath.pathWithPGCatalogAndDoc()),
                    "Sets the schema search order.",
                    DataTypes.STRING.getName()))
            .put(HASH_JOIN_KEY,
                new SessionSetting<>(
                    objects -> {
                        if (objects.length != 1) {
                            throw new IllegalArgumentException(HASH_JOIN_KEY + " should have only one argument.");
                        }
                    },
                    objects -> DataTypes.BOOLEAN.value(objects[0]),
                    SessionContext::setHashJoinEnabled,
                    s -> Boolean.toString(s.hashJoinsEnabled()),
                    () -> String.valueOf(true),
                    "Considers using the Hash Join instead of the Nested Loop Join implementation.",
                    DataTypes.BOOLEAN.getName()))
            .put(MAX_INDEX_KEYS,
                new SessionSetting<>(
                    objects -> {},
                    Function.identity(),
                    (s, v) -> {
                        throw new UnsupportedOperationException("\"" + MAX_INDEX_KEYS + "\" cannot be changed.");
                    },
                    s -> String.valueOf(32),
                    () -> String.valueOf(32),
                    "Shows the maximum number of index keys.",
                    DataTypes.INTEGER.getName()))
            .put(
                SERVER_VERSION_NUM,
                new SessionSetting<>(
                    objects -> {},
                    Function.identity(),
                    (s, v) -> {
                        throw new UnsupportedOperationException("\"" + SERVER_VERSION_NUM + "\" cannot be changed.");
                    },
                    s -> String.valueOf(PostgresWireProtocol.SERVER_VERSION_NUM),
                    () -> String.valueOf(PostgresWireProtocol.SERVER_VERSION_NUM),
                    "Reports the emulated PostgreSQL version number",
                    DataTypes.INTEGER.getName()
                )
            )
            .put(
                SERVER_VERSION,
                new SessionSetting<>(
                    objects -> {},
                    Function.identity(),
                    (s, v) -> {
                        throw new UnsupportedOperationException("\"" + SERVER_VERSION + "\" cannot be changed.");
                    },
                    s -> String.valueOf(PostgresWireProtocol.PG_SERVER_VERSION),
                    () -> String.valueOf(PostgresWireProtocol.PG_SERVER_VERSION),
                    "Reports the emulated PostgreSQL version number",
                    DataTypes.STRING.getName()
                )
            )
        .putAll(generateOptimizerRuleEntries())
        .build();

    private static Map<String, SessionSetting<?>> generateOptimizerRuleEntries() {
        var result = new HashMap<String, SessionSetting<?>>();
        for (var rule : Rule.IMPLEMENTATIONS) {
            var fullName = rule.getName();
            var simpleName = rule.getSimpleName();
            var optimizerRuleName = OPTIMIZER_RULE + CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE,
                                                                                  simpleName);
            result.put(optimizerRuleName, new SessionSetting<>(
                objects -> {},
                objects -> DataTypes.BOOLEAN.value(objects[0]),
                (sessionContext, includeRule) -> {
                    if (includeRule) {
                        sessionContext.excludedOptimizerRules().remove(fullName);
                    } else {
                        sessionContext.excludedOptimizerRules().add(fullName);
                    }
                },
                s -> String.valueOf(s.excludedOptimizerRules().contains(fullName) == false),
                () -> String.valueOf(true),
                String.format("Indicates if the optimizer rule %s is activated.", simpleName),
                DataTypes.BOOLEAN.getName()));
        }
        return result;
    }

    private static String[] objectsToStringArray(Object[] objects) {
        String[] strings = new String[objects.length];
        for (int i = 0; i < objects.length; i++) {
            strings[i] = DataTypes.STRING.value(objects[i]);
        }
        return strings;
    }

    private static String iterableToString(Iterable<String> iterable) {
        Iterator<String> it = iterable.iterator();
        StringBuilder sb = new StringBuilder();
        while (it.hasNext()) {
            sb.append(it.next());
            if (it.hasNext()) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
