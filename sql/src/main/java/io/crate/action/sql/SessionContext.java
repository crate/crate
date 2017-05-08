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

package io.crate.action.sql;

import com.google.common.base.MoreObjects;
import io.crate.metadata.Schemas;
import io.crate.operation.user.User;

import javax.annotation.Nullable;
import java.util.Properties;
import java.util.Set;

public class SessionContext {

    public static final SessionContext SYSTEM_SESSION = new SessionContext(0, Option.NONE, null, null);

    private final int defaultLimit;
    private final Set<Option> options;
    private String defaultSchema;
    private final User user;

    public SessionContext(Properties properties, @Nullable User user) {
        this(0, Option.NONE, properties.getProperty("database"), user);
    }

    public SessionContext(int defaultLimit, Set<Option> options, @Nullable String defaultSchema, @Nullable User user) {
        this.defaultLimit = defaultLimit;
        this.options = options;
        this.user = user;
        setDefaultSchema(defaultSchema);
    }

    public Set<Option> options() {
        return options;
    }

    public String defaultSchema() {
        return defaultSchema;
    }

    public void setDefaultSchema(@Nullable String schema) {
        defaultSchema = MoreObjects.firstNonNull(schema, Schemas.DEFAULT_SCHEMA_NAME);
    }

    @Nullable
    public User user() {
        return user;
    }

    public int defaultLimit() {
        return defaultLimit;
    }
}
