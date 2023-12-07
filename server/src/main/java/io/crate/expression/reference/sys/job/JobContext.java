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

package io.crate.expression.reference.sys.job;

import java.util.UUID;

import org.jetbrains.annotations.Nullable;

import io.crate.planner.operators.StatementClassifier.Classification;
import io.crate.user.Role;

public class JobContext {

    private final UUID id;
    private final String username;
    private final String stmt;
    private final long started;
    @Nullable
    private final Classification classification;

    public JobContext(UUID id, String stmt, long started, Role user, @Nullable Classification classification) {
        this.id = id;
        this.stmt = stmt;
        this.started = started;
        this.username = user.name();
        this.classification = classification;
    }

    public UUID id() {
        return id;
    }

    public String stmt() {
        return stmt;
    }

    public String username() {
        return username;
    }

    public long started() {
        return started;
    }

    @Nullable
    public Classification classification() {
        return classification;
    }

    @Override
    public String toString() {
        return "JobContext{" +
               "id=" + id +
               ", username='" + username + '\'' +
               ", stmt='" + stmt + '\'' +
               ", started=" + started +
               ", classification=" + classification +
               '}';
    }
}
