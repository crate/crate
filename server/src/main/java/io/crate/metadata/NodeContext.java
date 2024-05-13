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

import java.util.function.Function;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import io.crate.role.Roles;

@Singleton
public class NodeContext {

    private final Functions functions;
    private final long serverStartTimeInMs;
    private final Roles roles;
    private final Function<NodeContext, Schemas> createSchemas;
    private Schemas schemas;

    @Inject
    public NodeContext(Functions functions, Roles roles, Function<NodeContext, Schemas> createSchemas) {
        this.functions = functions;
        this.serverStartTimeInMs = SystemClock.currentInstant().toEpochMilli();
        this.roles = roles;
        this.createSchemas = createSchemas;
    }

    public Functions functions() {
        return functions;
    }

    public long serverStartTimeInMs() {
        return serverStartTimeInMs;
    }

    public Roles roles() {
        return roles;
    }

    public Schemas schemas() {
        if (schemas == null) {
            synchronized (createSchemas) {
                if (schemas == null) {
                    schemas = createSchemas.apply(this);
                }
            }
        }
        return schemas;
    }

    public NodeContext copy() {
        NodeContext copy = new NodeContext(functions, roles, createSchemas);
        copy.schemas = this.schemas;
        return copy;
    }
}
