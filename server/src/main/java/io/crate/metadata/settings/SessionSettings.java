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

import java.io.IOException;
import java.util.Objects;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.unit.TimeValue;
import io.crate.metadata.SearchPath;

/**
 * Streamable session settings.
 * This is a subset of {@link CoordinatorSessionSettings} containing only getters for settings
 * which can influence the execution on a collect or merge node.
 */
public class SessionSettings implements Writeable {

    protected String userName;
    protected SearchPath searchPath;
    protected boolean hashJoinsEnabled;
    protected boolean errorOnUnknownObjectKey;
    protected int memoryLimit;

    @VisibleForTesting
    public SessionSettings(String userName, SearchPath searchPath) {
        this(userName, searchPath, true, true, 0);
    }

    public SessionSettings(String userName,
                           SearchPath searchPath,
                           boolean hashJoinsEnabled,
                           boolean errorOnUnknownObjectKey,
                           int memoryLimit) {
        this.userName = userName;
        this.searchPath = searchPath;
        this.hashJoinsEnabled = hashJoinsEnabled;
        this.errorOnUnknownObjectKey = errorOnUnknownObjectKey;
        this.memoryLimit = memoryLimit;
    }


    public SessionSettings(StreamInput in) throws IOException {
        this.userName = in.readString();
        this.searchPath = SearchPath.createSearchPathFrom(in);
        this.hashJoinsEnabled = in.readBoolean();
        Version version = in.getVersion();
        if (version.onOrAfter(Version.V_4_7_0)) {
            this.errorOnUnknownObjectKey = in.readBoolean();
        } else {
            this.errorOnUnknownObjectKey = true;
        }
        if (version.onOrAfter(Version.V_5_5_0)) {
            this.memoryLimit = in.readVInt();
        } else {
            this.memoryLimit = 0;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(userName);
        searchPath.writeTo(out);
        out.writeBoolean(hashJoinsEnabled);
        Version version = out.getVersion();
        if (version.onOrAfter(Version.V_4_7_0)) {
            out.writeBoolean(errorOnUnknownObjectKey);
        }
        if (version.onOrAfter(Version.V_5_5_0)) {
            out.writeVInt(memoryLimit);
        }
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

    public String applicationName() {
        // Only available on coordinator.
        return null;
    }

    public String dateStyle() {
        // Only available on coordinator.
        return null;
    }

    public TimeValue statementTimeout() {
        // Only available on coordinator
        return TimeValue.ZERO;
    }

    /**
     * memory.operation_limit
     **/
    public int memoryLimitInBytes() {
        return memoryLimit;
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
               Objects.equals(hashJoinsEnabled, that.hashJoinsEnabled) &&
               Objects.equals(memoryLimit, that.memoryLimit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userName, searchPath, hashJoinsEnabled, memoryLimit);
    }
}
