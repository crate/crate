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

package io.crate.metadata.settings;

import io.crate.metadata.SearchPath;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class SessionSettings implements Writeable {

    private final String userName;
    private final Map<String, String> sessionSettings;

    private SearchPath searchPath;


    public SessionSettings(StreamInput in) throws IOException {
        this.userName = in.readString();

        final int numOfSettings = in.readVInt();
        Map<String, String> sessionSettings = new HashMap<>();
        for (int i = 0; i < numOfSettings; i++) {
            sessionSettings.put(in.readString(), in.readString());
        }
        this.sessionSettings = sessionSettings;
        setSettings();
    }

    public SessionSettings(String userName,
                           Map<String, String> sessionSettings) {
        this.userName = userName;
        this.sessionSettings = sessionSettings;
        setSettings();
    }

    private void setSettings() {
        final String serialisedSearchPath = sessionSettings.get(SessionSettingRegistry.SEARCH_PATH_KEY);
        this.searchPath = (serialisedSearchPath != null)
            ? SearchPath.createSearchPathFrom(serialisedSearchPath.split(","))
            : SearchPath.createSearchPathFrom((String) null);
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

    public Map<String, String> sessionSettings() {
        return sessionSettings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(userName);

        out.writeVInt(sessionSettings.size());
        for (var entry : sessionSettings.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
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
               Objects.equals(sessionSettings, that.sessionSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userName, searchPath, sessionSettings);
    }
}
