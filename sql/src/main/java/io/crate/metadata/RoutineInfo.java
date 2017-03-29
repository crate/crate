/*
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate.io licenses
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
 * To enable or use any of the enterprise features, Crate.io must have given
 * you permission to enable and use the Enterprise Edition of CrateDB and you
 * must have a valid Enterprise or Subscription Agreement with Crate.io.  If
 * you enable or use features that are part of the Enterprise Edition, you
 * represent and warrant that you have a valid Enterprise or Subscription
 * Agreement with Crate.io.  Your use of features of the Enterprise Edition
 * is governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.metadata;

import org.elasticsearch.common.Nullable;

import javax.annotation.Nonnull;

public class RoutineInfo {
    private final String schema;
    private final String name;
    private final String type;
    private final String body;
    private final String dataType;
    private final boolean isDeterministic;
    private final String definition;

    public RoutineInfo(String name,
                       String type,
                       @Nullable String schema,
                       @Nullable String definition,
                       @Nullable String body,
                       @Nullable String dataType,
                       @Nullable boolean isDeterministic) {
        assert name != null : "name must not be null";
        assert type != null : "type must not be null";
        this.name = name;
        this.schema = schema;
        this.type = type;
        this.body = body;
        this.dataType = dataType;
        this.definition = definition;
        this.isDeterministic = isDeterministic;
    }

    public RoutineInfo(String name, String type) {
        assert name != null : "name must not be null";
        assert type != null : "type must not be null";
        this.name = name;
        this.type = type;
        this.schema = null;
        this.body = null;
        this.dataType = null;
        this.definition = null;
        this.isDeterministic = false;
    }

    @Nonnull
    public String name() {
        return name;
    }

    @Nonnull
    public String type() {
        return type;
    }

    public String body() {
        return body;
    }

    public String schema() {
        return schema;
    }

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
