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

package org.elasticsearch.action.admin.indices.settings.put;

import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;
import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.elasticsearch.Version;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * Request for an update index settings action
 */
public class UpdateSettingsRequest extends AcknowledgedRequest<UpdateSettingsRequest>
        implements IndicesRequest, ToXContentObject {

    private String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, false, true, true);
    private Settings settings = EMPTY_SETTINGS;

    /**
     * Constructs a new request to update settings for one or more indices
     */
    public UpdateSettingsRequest(Settings settings, String... indices) {
        this.indices = indices;
        this.settings = settings;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    public Settings settings() {
        return settings;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * Sets the settings to be updated (either json or yaml format)
     */
    public UpdateSettingsRequest settings(String source, XContentType xContentType) {
        this.settings = Settings.builder().loadFromSource(source, xContentType).build();
        return this;
    }

    public UpdateSettingsRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        settings = readSettingsFromStream(in);
        if (in.getVersion().before(Version.V_5_8_0)) {
            in.readBoolean(); // preserveExisting
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(indices);
        indicesOptions.writeIndicesOptions(out);
        writeSettingsToStream(out, settings);
        if (out.getVersion().before(Version.V_5_8_0)) {
            out.writeBoolean(false); // preserveExisting
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        settings.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "indices : " + Arrays.toString(indices) + "," + Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UpdateSettingsRequest that = (UpdateSettingsRequest) o;
        return masterNodeTimeout.equals(that.masterNodeTimeout)
                && timeout.equals(that.timeout)
                && Objects.equals(settings, that.settings)
                && Objects.equals(indicesOptions, that.indicesOptions)
                && Arrays.equals(indices, that.indices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(masterNodeTimeout, timeout, settings, indicesOptions, Arrays.hashCode(indices));
    }

}
