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
import java.util.Objects;
import java.util.Set;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

/**
 * Alter repository request.
 * <p>
 * Alters an existing repository by adding the specified settings or replacing them
 * if they are already set.
 */
public class AlterRepositoryRequest extends AcknowledgedRequest<AlterRepositoryRequest> {

    // When adding or removing fields, don't forget to update AlterRepositoryRequestTest!

    private final String name;
    private final Settings settings;

    public AlterRepositoryRequest(String name, Settings settings) {
        this.name = name;
        this.settings = settings;
    }

    public AlterRepositoryRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
        settings = readSettingsFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        writeSettingsToStream(out, settings);
    }

    public String name() {
        return this.name;
    }

    public Settings settings() {
        return this.settings;
    }

    public Set<String> resetProperties() {
        return this.settings.keySet();
    }

    @Override
    public String toString() {
        return "AlterRepositoryRequest{" +
            "name=" + name + ";" +
            "settings=" + settings.toString() +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AlterRepositoryRequest that)) return false;
        return Objects.equals(name, that.name) &&
            Objects.equals(settings, that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, settings);
    }
}
