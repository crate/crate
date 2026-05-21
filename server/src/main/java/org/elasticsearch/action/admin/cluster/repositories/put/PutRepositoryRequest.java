/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.repositories.put;

import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

/**
 * Register repository request.
 * <p>
 * Registers a repository with given name, type and settings. The request fails if a repository with the same name
 * already exists in the cluster.
 */
public class PutRepositoryRequest extends AcknowledgedRequest<PutRepositoryRequest> {

    private final String name;
    private final String type;
    private final Settings settings;

    public PutRepositoryRequest(String name, String type, Settings settings) {
        this.name = name;
        this.type = type;
        this.settings = settings;
    }

    public PutRepositoryRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
        type = in.readString();
        settings = readSettingsFromStream(in);
        if (in.getVersion().before(Version.V_6_4_0)) {
            in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        out.writeString(type);
        writeSettingsToStream(out, settings);
        if (out.getVersion().before(Version.V_6_4_0)) {
            out.writeBoolean(true);
        }
    }

    public String name() {
        return this.name;
    }

    public String type() {
        return this.type;
    }

    public Settings settings() {
        return this.settings;
    }
}
