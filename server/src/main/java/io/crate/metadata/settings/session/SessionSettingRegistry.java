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

package io.crate.metadata.settings.session;

import static io.crate.Constants.DEFAULT_DATE_STYLE;
import static io.crate.metadata.SearchPath.createSearchPathFrom;

import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.joda.time.Period;
import org.joda.time.PeriodType;

import io.crate.action.sql.Sessions;
import io.crate.common.collections.MapBuilder;
import io.crate.common.unit.TimeValue;
import io.crate.metadata.IndexName;
import io.crate.metadata.SearchPath;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.settings.SessionSettings;
import io.crate.protocols.postgres.PostgresWireProtocol;
import io.crate.types.BooleanType;
import io.crate.types.DataTypes;

@Singleton
public class SessionSettingRegistry {

    private static final String SEARCH_PATH_KEY = "search_path";
    public static final String HASH_JOIN_KEY = "enable_hashjoin";
    static final String MAX_INDEX_KEYS = "max_index_keys";
    static final String MAX_IDENTIFIER_LENGTH = "max_identifier_length";
    static final String SERVER_VERSION_NUM = "server_version_num";
    static final String SERVER_VERSION = "server_version";
    static final String STANDARD_CONFORMING_STRINGS = "standard_conforming_strings";
    static final String ERROR_ON_UNKNOWN_OBJECT_KEY = "error_on_unknown_object_key";
    static final String DATE_STYLE_KEY = "datestyle";
    static final String APPLICATION_NAME_KEY = "application_name";

    static final SessionSetting<String> APPLICATION_NAME = new SessionSetting<>(
        APPLICATION_NAME_KEY,
        inputs -> DataTypes.STRING.implicitCast(inputs[0]),
        CoordinatorSessionSettings::setApplicationName,
        SessionSettings::applicationName,
        () -> null,
        "Optional application name. Can be set by a client to identify the application which created the connection",
        DataTypes.STRING
    );
    static final SessionSetting<String> DATE_STYLE = new SessionSetting<>(
        DATE_STYLE_KEY,
        inputs -> {
            validateDateStyleFrom(objectsToStringArray(inputs));
            return DEFAULT_DATE_STYLE;
        },
        CoordinatorSessionSettings::setDateStyle,
        SessionSettings::dateStyle,
        () -> DEFAULT_DATE_STYLE,
        "Display format for date and time values.",
        DataTypes.STRING
    );

    static final SessionSetting<TimeValue> STATEMENT_TIMEOUT = new SessionSetting<>(
        Sessions.STATEMENT_TIMEOUT_KEY,
        inputs -> {
            Object input = inputs[0];
            // Interpret values without explicit unit/interval format as milliseconds for PostgreSQL compat.
            if (input instanceof Number num) {
                return TimeValue.timeValueMillis(num.longValue());
            }
            if (input instanceof String str) {
                try {
                    int millis = Integer.parseInt(str);
                    return TimeValue.timeValueMillis(millis);
                } catch (NumberFormatException ignored) {
                    // continue with implicitCast
                }
            }
            // Must fit into `Integer` range as millis
            Period period = DataTypes.INTERVAL.implicitCast(input)
                .normalizedStandard(PeriodType.millis());
            return TimeValue.timeValueMillis(period.getMillis());
        },
        CoordinatorSessionSettings::statementTimeout,
        settings -> settings.statementTimeout().toString(),
        () -> "0",
        "The maximum duration of any statement before it gets killed. Infinite/disabled if 0",
        DataTypes.INTERVAL
    );

    static final SessionSetting<Integer> MEMORY_LIMIT = new SessionSetting<>(
        Sessions.MEMORY_LIMIT_KEY,
        inputs -> DataTypes.INTEGER.implicitCast(inputs[0]),
        CoordinatorSessionSettings::memoryLimit,
        settings -> Integer.toString(settings.memoryLimitInBytes()),
        () -> "0",
        "Memory limit in bytes for an individual operation. 0 by-passes the operation limit, relying entirely on the global circuit breaker limits",
        DataTypes.INTEGER
    );

    private final Map<String, SessionSetting<?>> settings;

    @Inject
    public SessionSettingRegistry(Set<SessionSettingProvider> sessionSettingProviders) {
        var builder = MapBuilder.<String, SessionSetting<?>>treeMapBuilder()
            .put(SEARCH_PATH_KEY,
                 new SessionSetting<>(
                     SEARCH_PATH_KEY,
                     // everything allowed, empty list (resulting by ``SET .. TO DEFAULT`` results in defaults
                     objects -> createSearchPathFrom(objectsToStringArray(objects)),
                     CoordinatorSessionSettings::setSearchPath,
                     s -> String.join(", ", s.searchPath().showPath()),
                     () -> String.join(", ", SearchPath.pathWithPGCatalogAndDoc().showPath()),
                     "Sets the schema search order.",
                     DataTypes.STRING))
            .put(HASH_JOIN_KEY,
                 new SessionSetting<>(
                     HASH_JOIN_KEY,
                     objects -> {
                         if (objects.length != 1) {
                             throw new IllegalArgumentException(HASH_JOIN_KEY + " should have only one argument.");
                         }
                         return DataTypes.BOOLEAN.implicitCast(objects[0]);
                     },
                     CoordinatorSessionSettings::setHashJoinEnabled,
                     s -> Boolean.toString(s.hashJoinsEnabled()),
                     () -> String.valueOf(true),
                     "Considers using the Hash Join instead of the Nested Loop Join implementation.",
                     DataTypes.BOOLEAN))
            .put(MAX_INDEX_KEYS,
                 new SessionSetting<>(
                     MAX_INDEX_KEYS,
                     _ -> {
                         throw new UnsupportedOperationException("\"" + MAX_INDEX_KEYS + "\" cannot be changed.");
                     },
                     (s, v) -> {},
                     s -> String.valueOf(32),
                     () -> String.valueOf(32),
                     "Shows the maximum number of index keys.",
                     DataTypes.INTEGER))
            .put(MAX_IDENTIFIER_LENGTH,
                 new SessionSetting<>(
                     MAX_IDENTIFIER_LENGTH,
                     _ -> {
                         throw new UnsupportedOperationException("\"" + MAX_IDENTIFIER_LENGTH + "\" cannot be changed.");
                     },
                     (s, v) -> {},
                     s -> String.valueOf(IndexName.MAX_INDEX_NAME_BYTES),
                     () -> String.valueOf(IndexName.MAX_INDEX_NAME_BYTES),
                     "Shows the maximum length of identifiers in bytes.",
                     DataTypes.INTEGER))
            .put(SERVER_VERSION_NUM,
                 new SessionSetting<>(
                     SERVER_VERSION_NUM,
                     _ -> {
                         throw new UnsupportedOperationException("\"" + SERVER_VERSION_NUM + "\" cannot be changed.");
                     },
                     (s, v) -> {},
                     s -> String.valueOf(PostgresWireProtocol.SERVER_VERSION_NUM),
                     () -> String.valueOf(PostgresWireProtocol.SERVER_VERSION_NUM),
                     "Reports the emulated PostgreSQL version number",
                     DataTypes.INTEGER
                 )
            )
            .put(SERVER_VERSION,
                 new SessionSetting<>(
                     SERVER_VERSION,
                     _ -> {
                         throw new UnsupportedOperationException("\"" + SERVER_VERSION + "\" cannot be changed.");
                     },
                     (s, v) -> {},
                     s -> String.valueOf(PostgresWireProtocol.PG_SERVER_VERSION),
                     () -> String.valueOf(PostgresWireProtocol.PG_SERVER_VERSION),
                     "Reports the emulated PostgreSQL version number",
                     DataTypes.STRING
                 )
            )
            .put(STANDARD_CONFORMING_STRINGS,
                 new SessionSetting<>(
                     STANDARD_CONFORMING_STRINGS,
                     objects -> {
                         if (objects.length != 1) {
                             throw new IllegalArgumentException(STANDARD_CONFORMING_STRINGS + " should have only one argument.");
                         }
                         validateStandardConformingStrings(objectsToStringArray(objects)[0]);
                         return objects;
                     },
                     (s, v) -> {},
                     s -> "on",
                     () -> "on",
                     "Causes '...' strings to treat backslashes literally.",
                     DataTypes.STRING
                 )
            )
            .put(ERROR_ON_UNKNOWN_OBJECT_KEY,
                 new SessionSetting<>(
                     ERROR_ON_UNKNOWN_OBJECT_KEY,
                     objects -> {
                         if (objects.length != 1) {
                             throw new IllegalArgumentException(ERROR_ON_UNKNOWN_OBJECT_KEY + " should have only one argument.");
                         }
                         return DataTypes.BOOLEAN.implicitCast(objects[0]);
                     },
                     CoordinatorSessionSettings::setErrorOnUnknownObjectKey,
                     s -> Boolean.toString(s.errorOnUnknownObjectKey()),
                     () -> String.valueOf(true),
                     "Raises or suppresses ObjectKeyUnknownException when querying nonexistent keys to dynamic objects.",
                     DataTypes.BOOLEAN)
            )
            .put(APPLICATION_NAME.name(), APPLICATION_NAME)
            .put(DATE_STYLE.name(), DATE_STYLE)
            .put(STATEMENT_TIMEOUT.name(), STATEMENT_TIMEOUT)
            .put(MEMORY_LIMIT.name(), MEMORY_LIMIT);

        for (var providers : sessionSettingProviders) {
            for (var setting : providers.sessionSettings()) {
                builder.put(setting.name(), setting);
            }
        }
        this.settings = builder.immutableMap();
    }

    public Map<String, SessionSetting<?>> settings() {
        return settings;
    }

    public Iterable<NamedSessionSetting> namedSessionSettings(TransactionContext txnCtx) {
        return () -> settings.entrySet().stream()
            .map(x -> new NamedSessionSetting(x.getKey(), x.getValue(), txnCtx))
            .iterator();
    }

    private static String[] objectsToStringArray(Object[] objects) {
        ArrayList<String> argumentList = new ArrayList<>();
        for (int i = 0; i < objects.length; i++) {
            String str = DataTypes.STRING.implicitCast(objects[i]);
            for (String element : str.split(",")) {
                argumentList.add(element.trim());
            }
        }
        return argumentList.toArray(String[]::new);
    }

    private static void validateDateStyleFrom(String... strings) {
        String dateStyle;
        for (String s : strings) {
            dateStyle = s.toUpperCase(Locale.ENGLISH);
            switch (dateStyle) {
                // date format style
                case "ISO":
                    break;
                case "SQL",
                     "POSTGRES",
                      "GERMAN":
                    throw new IllegalArgumentException("Invalid value for parameter \"" + DATE_STYLE.name() + "\": \"" +
                                                        dateStyle + "\". Valid values include: [\"ISO\"].");
                // date order style
                case "MDY",
                     "NONEURO",
                     "NONEUROPEAN",
                     "US",
                     "DMY",
                     "EURO",
                     "EUROPEAN",
                     "YMD":
                    break;
                default:
                    throw new IllegalArgumentException("Invalid value for parameter \"" + DATE_STYLE.name() + "\": \"" +
                                                       dateStyle + "\". Valid values include: [\"ISO\"].");
            }
        }
    }

    private static void validateStandardConformingStrings(String str) {
        if (BooleanType.INSTANCE.implicitCast(str) == Boolean.FALSE) {
            throw new UnsupportedOperationException("\"" + STANDARD_CONFORMING_STRINGS + "\" cannot be changed.");
        }
    }
}
