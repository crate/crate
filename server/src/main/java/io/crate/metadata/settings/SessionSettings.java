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

package io.crate.metadata.settings;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.metadata.SearchPath;
import io.crate.planner.optimizer.Rule;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

public final class SessionSettings implements Writeable {

    private final String userName;
    private final SearchPath searchPath;
    private final boolean hashJoinsEnabled;
    private final boolean errorOnUnknownObjectKey;
    private final Set<Class<? extends Rule<?>>> excludedOptimizerRules;

    public SessionSettings(StreamInput in) throws IOException {
        this.userName = in.readString();
        this.searchPath = SearchPath.createSearchPathFrom(in);
        this.hashJoinsEnabled = in.readBoolean();
        // excludedOptimizerRules are only used on the coordinator node
        // and never needed any other node and therefore are excluded from
        // serialization on purpose.
        this.excludedOptimizerRules = Set.of();
        if (in.getVersion().onOrAfter(Version.V_4_7_0)) {
            this.errorOnUnknownObjectKey = in.readBoolean();
        } else {
            this.errorOnUnknownObjectKey = true;
        }
    }

    @VisibleForTesting
    public SessionSettings(String userName, SearchPath searchPath) {
        this(userName, searchPath, true, Set.of(), true);
    }

    public SessionSettings(String userName,
                           SearchPath searchPath,
                           boolean hashJoinsEnabled,
                           Set<Class<? extends Rule<?>>> rules,
                           boolean errorOnUnknownObjectKey) {
        this.userName = userName;
        this.searchPath = searchPath;
        this.hashJoinsEnabled = hashJoinsEnabled;
        this.excludedOptimizerRules = rules;
        this.errorOnUnknownObjectKey = errorOnUnknownObjectKey;
    }

    public String userName() {
        return userName;
    }

    public String currentSchema() {
        return searchPath.currentSchema();
    }

    public SearchPath searchPath() {
        return searchPath;
    }

    public boolean hashJoinsEnabled() {
        return hashJoinsEnabled;
    }

    public boolean errorOnUnknownObjectKey() {
        return errorOnUnknownObjectKey;
    }

    public Set<Class<? extends Rule<?>>> excludedOptimizerRules() {
        return excludedOptimizerRules;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(userName);
        searchPath.writeTo(out);
        out.writeBoolean(hashJoinsEnabled);
        if (out.getVersion().onOrAfter(Version.V_4_7_0)) {
            out.writeBoolean(errorOnUnknownObjectKey);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SessionSettings that = (SessionSettings) o;
        return Objects.equals(userName, that.userName) &&
               Objects.equals(searchPath, that.searchPath) &&
               Objects.equals(hashJoinsEnabled, that.hashJoinsEnabled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userName, searchPath, hashJoinsEnabled);
    }
}
