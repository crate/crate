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

package io.crate.metadata;

import org.jspecify.annotations.Nullable;

public class RoutineInfo {

    @Nullable
    private final String specificName;

    @Nullable
    private final String schema;

    private final String name;

    private final String type;

    @Nullable
    private final String body;

    @Nullable
    private final String dataType;

    private final boolean isDeterministic;

    @Nullable
    private final String definition;

    public RoutineInfo(String name,
                       String type,
                       @Nullable String schema,
                       @Nullable String specificName,
                       @Nullable String definition,
                       @Nullable String body,
                       @Nullable String dataType,
                       boolean isDeterministic) {
        assert name != null : "name must not be null";
        assert type != null : "type must not be null";
        this.specificName = specificName;
        this.name = name;
        this.schema = schema;
        this.type = type;
        this.body = body;
        this.dataType = dataType;
        this.definition = definition;
        this.isDeterministic = isDeterministic;
    }

    public RoutineInfo(String name, String type) {
        this(name, type, null, null, null, null, null, false);
    }

    public RoutineInfo(String name, String type, @Nullable String definition) {
        this(name, type, null, null, definition, null, null, false);
    }

    public String name() {
        return name;
    }

    public String type() {
        return type;
    }

    public String body() {
        return body;
    }

    public String schema() {
        return schema;
    }

    public String specificName() {
        return specificName;
    }

    @Nullable
    public String definition() {
        return definition;
    }

    public String dataType() {
        return dataType;
    }

    public Boolean isDeterministic() {
        return isDeterministic;
    }
}
