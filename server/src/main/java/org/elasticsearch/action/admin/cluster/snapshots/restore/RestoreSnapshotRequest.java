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

package org.elasticsearch.action.admin.cluster.snapshots.restore;

import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import io.crate.analyze.SnapshotSettings;

/**
 * Restore snapshot request
 */
public class RestoreSnapshotRequest extends MasterNodeRequest<RestoreSnapshotRequest> {

    private final String snapshot;
    private final String repository;

    private final IndicesOptions indicesOptions;
    private final boolean includeAliases;
    private final Settings settings;

    private final boolean includeIndices;
    private final boolean includeCustomMetadata;
    private final String[] customMetadataTypes;
    private final boolean includeGlobalSettings;
    private final String[] globalSettings;

    private final List<TableOrPartition> tablesToRestore;


    /**
     * Constructs a new put repository request with the provided repository and snapshot names.
     *
     * @param repository repository name
     * @param snapshot   snapshot name
     */
    public RestoreSnapshotRequest(String repository,
                                  String snapshot,
                                  List<TableOrPartition> tablesToRestore,
                                  IndicesOptions indicesOptions,
                                  Settings settings,
                                  boolean includeTables,
                                  boolean includeCustomMetadata,
                                  Set<String> metadataTypes,
                                  boolean includeGlobalSettings,
                                  List<String> globalSettings) {
        this.repository = repository;
        this.snapshot = snapshot;
        this.tablesToRestore = tablesToRestore;
        this.indicesOptions = indicesOptions;
        this.settings = settings;
        this.includeIndices = includeTables;
        this.includeAliases = includeTables;
        this.includeCustomMetadata = includeCustomMetadata;
        this.customMetadataTypes = metadataTypes.toArray(String[]::new);
        this.includeGlobalSettings = includeGlobalSettings;
        this.globalSettings = globalSettings.toArray(String[]::new);
    }

    /**
     * Returns the name of the snapshot.
     *
     * @return snapshot name
     */
    public String snapshot() {
        return this.snapshot;
    }

    /**
     * Returns repository name
     *
     * @return repository name
     */
    public String repository() {
        return this.repository;
    }

    public List<TableOrPartition> tablesToRestore() {
        return tablesToRestore;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @return the desired behaviour regarding indices to ignore and wildcard indices expression
     */
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * Returns rename pattern
     *
     * @return rename pattern
     */
    public String tableRenamePattern() {
        return SnapshotSettings.TABLE_RENAME_PATTERN.get(settings);
    }

    /**
     * Returns table rename replacement
     *
     * @return table rename replacement
     */
    public String tableRenameReplacement() {
        return SnapshotSettings.TABLE_RENAME_REPLACEMENT.get(settings);
    }

    public String schemaRenamePattern() {
        return SnapshotSettings.SCHEMA_RENAME_PATTERN.get(settings);
    }

    public String schemaRenameReplacement() {
        return SnapshotSettings.SCHEMA_RENAME_REPLACEMENT.get(settings);
    }

    /**
     * Returns wait for completion setting
     *
     * @return true if the operation will wait for completion
     */
    public boolean waitForCompletion() {
        return SnapshotSettings.WAIT_FOR_COMPLETION.get(settings);
    }

    /**
     * Returns repository-specific restore settings
     *
     * @return restore settings
     */
    public Settings settings() {
        return this.settings;
    }

    /**
     * Returns true if aliases should be restored from this snapshot
     *
     * @return true if aliases should be restored
     */
    public boolean includeAliases() {
        return includeAliases;
    }

    public boolean includeIndices() {
        return includeIndices;
    }

    public boolean includeCustomMetadata() {
        return includeCustomMetadata;
    }

    public String[] customMetadataTypes() {
        return customMetadataTypes;
    }

    public boolean includeGlobalSettings() {
        return includeGlobalSettings;
    }

    public String[] globalSettings() {
        return globalSettings;
    }

    public RestoreSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        snapshot = in.readString();
        repository = in.readString();
        Version version = in.getVersion();
        assert version.onOrAfter(Version.V_5_10_0) : "Rolling upgrade is only supported for 5.10 -> 6.0";

        if (version.before(Version.V_6_0_0)) {
            in.readStringArray(); // indices
        }
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        if (version.before(Version.V_6_0_0)) {
            // Since 5.6 there are default values which behave similarly as pre-5.6 with NULL values.
            // tableRenamePattern
            in.readString();
            // tableRenameReplacement
            in.readString();
            // waitForCompletion
            in.readBoolean();
            // partial
            in.readBoolean();
        }
        includeAliases = in.readBoolean();
        settings = readSettingsFromStream(in);
        if (version.before(Version.V_6_0_0)) {
            readSettingsFromStream(in); // indexSettings
            in.readStringArray(); // ignoreIndexSettings
            in.readStringArray(); // templates
        }
        includeIndices = in.readBoolean();
        includeCustomMetadata = in.readBoolean();
        customMetadataTypes = in.readStringArray();
        includeGlobalSettings = in.readBoolean();
        globalSettings = in.readStringArray();
        tablesToRestore = in.readList(TableOrPartition::new);
        if (version.before(Version.V_6_0_0)) {
            // schemaRenamePattern
            in.readString();
            // schemaRenameReplacement
            in.readString();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(snapshot);
        out.writeString(repository);
        Version version = out.getVersion();
        assert version.onOrAfter(Version.V_5_10_0) : "Rolling upgrade is only supported for 5.10 -> 6.0";

        if (version.before(Version.V_6_0_0)) {
            // indices
            out.writeStringArray(Strings.EMPTY_ARRAY);
        }
        indicesOptions.writeIndicesOptions(out);
        if (version.before(Version.V_6_0_0)) {
            out.writeString(tableRenamePattern());
            out.writeString(tableRenameReplacement());
            out.writeBoolean(waitForCompletion());
            out.writeBoolean(false); // partial; was never set to true
        }
        out.writeBoolean(includeAliases);
        writeSettingsToStream(out, settings);
        if (version.before(Version.V_6_0_0)) {
            writeSettingsToStream(out, Settings.EMPTY); // indexSettings
            out.writeStringArray(Strings.EMPTY_ARRAY); // ignoreIndexSettings
            out.writeStringArray(Strings.EMPTY_ARRAY); // templates
        }
        out.writeBoolean(includeIndices);
        out.writeBoolean(includeCustomMetadata);
        out.writeStringArray(customMetadataTypes);
        out.writeBoolean(includeGlobalSettings);
        out.writeStringArray(globalSettings);
        out.writeCollection(tablesToRestore);
        if (version.before(Version.V_6_0_0)) {
            out.writeString(schemaRenamePattern());
            out.writeString(schemaRenameReplacement());
        }
    }

    @Override
    public String getDescription() {
        return "snapshot [" + repository + ":" + snapshot + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RestoreSnapshotRequest that = (RestoreSnapshotRequest) o;
        return includeAliases == that.includeAliases &&
            Objects.equals(snapshot, that.snapshot) &&
            Objects.equals(repository, that.repository) &&
            Objects.equals(indicesOptions, that.indicesOptions) &&
            Objects.equals(settings, that.settings) &&
            includeIndices == that.includeIndices &&
            includeCustomMetadata == that.includeCustomMetadata &&
            Arrays.equals(customMetadataTypes, that.customMetadataTypes) &&
            includeGlobalSettings == that.includeGlobalSettings &&
            Arrays.equals(globalSettings, that.globalSettings);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
            snapshot,
            repository,
            indicesOptions,
            includeAliases,
            settings,
            includeIndices,
            includeCustomMetadata,
            includeGlobalSettings
        );
        result = 31 * result + Arrays.hashCode(customMetadataTypes);
        result = 31 * result + Arrays.hashCode(globalSettings);
        return result;
    }
}
