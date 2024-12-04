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
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.RelationName;

/**
 * Restore snapshot request
 */
public class RestoreSnapshotRequest extends MasterNodeRequest<RestoreSnapshotRequest> {

    private String snapshot;
    private String repository;

    @Deprecated
    private String[] indices = Strings.EMPTY_ARRAY;

    @Deprecated
    private String[] templates = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
    private String tableRenamePattern;
    private String tableRenameReplacement;
    private String schemaRenamePattern;
    private String schemaRenameReplacement;
    private boolean waitForCompletion;
    private boolean includeGlobalState = false;
    private boolean partial = false;
    private boolean includeAliases = true;
    private Settings settings = Settings.EMPTY;
    private Settings indexSettings = Settings.EMPTY;
    private String[] ignoreIndexSettings = Strings.EMPTY_ARRAY;

    private boolean includeIndices = true;
    private boolean includeCustomMetadata = false;
    private String[] customMetadataTypes = Strings.EMPTY_ARRAY;
    private boolean includeGlobalSettings = false;
    private String[] globalSettings = Strings.EMPTY_ARRAY;

    private List<TableOrPartition> tablesToRestore = List.of();



    public record TableOrPartition(RelationName table, @Nullable String partitionIdent) implements Writeable {

        public TableOrPartition(StreamInput in) throws IOException {
            this(new RelationName(in), in.readOptionalString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            table.writeTo(out);
            out.writeOptionalString(partitionIdent);
        }
    }

    public RestoreSnapshotRequest() {
    }

    /**
     * Constructs a new put repository request with the provided repository and snapshot names.
     *
     * @param repository repository name
     * @param snapshot   snapshot name
     */
    public RestoreSnapshotRequest(String repository, String snapshot) {
        this.snapshot = snapshot;
        this.repository = repository;
    }

    /**
     * Sets the name of the snapshot.
     *
     * @param snapshot snapshot name
     * @return this request
     */
    public RestoreSnapshotRequest snapshot(String snapshot) {
        this.snapshot = snapshot;
        return this;
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
     * Sets repository name
     *
     * @param repository repository name
     * @return this request
     */
    public RestoreSnapshotRequest repository(String repository) {
        this.repository = repository;
        return this;
    }

    /**
     * Returns repository name
     *
     * @return repository name
     */
    public String repository() {
        return this.repository;
    }

    /**
     * Sets the list of indices that should be restored from snapshot
     * <p>
     * The list of indices supports multi-index syntax. For example: "+test*" ,"-test42" will index all indices with
     * prefix "test" except index "test42". Aliases are not supported. An empty list or {"_all"} will restore all open
     * indices in the snapshot.
     *
     * @param indices list of indices
     * @return this request
     */
    public RestoreSnapshotRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Sets the list of indices that should be restored from snapshot
     * <p>
     * The list of indices supports multi-index syntax. For example: "+test*" ,"-test42" will index all indices with
     * prefix "test" except index "test42". Aliases are not supported. An empty list or {"_all"} will restore all open
     * indices in the snapshot.
     *
     * @param indices list of indices
     * @return this request
     */
    public RestoreSnapshotRequest indices(List<String> indices) {
        this.indices = indices.toArray(new String[indices.size()]);
        return this;
    }

    /**
     * Returns list of indices that should be restored from snapshot
     */
    public String[] indices() {
        return indices;
    }

    public RestoreSnapshotRequest templates(String... templates) {
        this.templates = templates;
        return this;
    }

    /**
     * Returns list of templates that should be restored from snapshot
     */
    public String[] templates() {
        return templates;
    }

    /**
     * Sets the list of tables that should be restored from snapshot
     * An empty list will restore all open indices in the snapshot.
     */
    public RestoreSnapshotRequest tablesToRestore(List<TableOrPartition> tablesToRestore) {
        this.tablesToRestore = tablesToRestore;
        return this;
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
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices to ignore and wildcard indices expressions
     * @return this request
     */
    public RestoreSnapshotRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    /**
     * Sets table rename pattern that should be applied to restored indices.
     * <p>
     * Indices that match the rename pattern will be renamed according to {@link #renameReplacement(String)}. The
     * rename pattern is applied according to the {@link java.util.regex.Matcher#appendReplacement(StringBuffer, String)}
     * The request will fail if two or more tables will be renamed into the same FQN.
     *
     * @param tableRenamePattern rename pattern
     * @return this request
     */
    public RestoreSnapshotRequest tableRenamePattern(String tableRenamePattern) {
        this.tableRenamePattern = tableRenamePattern;
        return this;
    }

    /**
     * Returns rename pattern
     *
     * @return rename pattern
     */
    public String tableRenamePattern() {
        return tableRenamePattern;
    }

    /**
     * Sets table rename replacement
     * <p>
     * See {@link #tableRenamePattern(String)} for more information.
     *
     * @param tableRenameReplacement rename replacement
     */
    public RestoreSnapshotRequest tableRenameReplacement(String tableRenameReplacement) {
        this.tableRenameReplacement = tableRenameReplacement;
        return this;
    }

    /**
     * Returns table rename replacement
     *
     * @return table rename replacement
     */
    public String tableRenameReplacement() {
        return tableRenameReplacement;
    }

    /**
     * Sets schema rename pattern that should be applied to restored indices.
     * <p>
     * Schemas that match the rename pattern will be renamed according to {@link #tableRenameReplacement(String)}. The
     * rename pattern is applied according to the {@link java.util.regex.Matcher#appendReplacement(StringBuffer, String)}
     * The request will fail if two or more tables will be renamed into the same FQN.
     *
     * @param schemaRenamePattern rename pattern
     * @return this request
     */
    public RestoreSnapshotRequest schemaRenamePattern(String schemaRenamePattern) {
        this.schemaRenamePattern = schemaRenamePattern;
        return this;
    }

    public String schemaRenamePattern() {
        return schemaRenamePattern;
    }

    /**
     * Sets schema rename replacement
     * <p>
     * See {@link #schemaRenamePattern()} (String)} for more information.
     */
    public RestoreSnapshotRequest schemaRenameReplacement(String schemaRenameReplacement) {
        this.schemaRenameReplacement = schemaRenameReplacement;
        return this;
    }

    public String schemaRenameReplacement() {
        return schemaRenameReplacement;
    }

    /**
     * If this parameter is set to true the operation will wait for completion of restore process before returning.
     *
     * @param waitForCompletion if true the operation will wait for completion
     * @return this request
     */
    public RestoreSnapshotRequest waitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
        return this;
    }

    /**
     * Returns wait for completion setting
     *
     * @return true if the operation will wait for completion
     */
    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    /**
     * Returns true if indices with failed to snapshot shards should be partially restored.
     *
     * @return true if indices with failed to snapshot shards should be partially restored
     */
    public boolean partial() {
        return partial;
    }

    /**
     * Set to true to allow indices with failed to snapshot shards should be partially restored.
     *
     * @param partial true if indices with failed to snapshot shards should be partially restored.
     * @return this request
     */
    public RestoreSnapshotRequest partial(boolean partial) {
        this.partial = partial;
        return this;
    }

    /**
     * Sets repository-specific restore settings.
     * <p>
     * See repository documentation for more information.
     *
     * @param settings repository-specific snapshot settings
     * @return this request
     */
    public RestoreSnapshotRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * Sets repository-specific restore settings.
     * <p>
     * See repository documentation for more information.
     *
     * @param settings repository-specific snapshot settings
     * @return this request
     */
    public RestoreSnapshotRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * Sets repository-specific restore settings in JSON or YAML format
     * <p>
     * See repository documentation for more information.
     *
     * @param source repository-specific snapshot settings
     * @param xContentType the content type of the source
     * @return this request
     */
    public RestoreSnapshotRequest settings(String source, XContentType xContentType) {
        this.settings = Settings.builder().loadFromSource(source, xContentType).build();
        return this;
    }

    /**
     * Sets repository-specific restore settings
     * <p>
     * See repository documentation for more information.
     *
     * @param source repository-specific snapshot settings
     * @return this request
     */
    public RestoreSnapshotRequest settings(Map<String, Object> source) {
        try {
            XContentBuilder builder = JsonXContent.builder();
            builder.map(source);
            settings(Strings.toString(builder), builder.contentType());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
        return this;
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
     * Sets the list of index settings and index settings groups that shouldn't be restored from snapshot
     */
    public RestoreSnapshotRequest ignoreIndexSettings(String... ignoreIndexSettings) {
        this.ignoreIndexSettings = ignoreIndexSettings;
        return this;
    }

    /**
     * Sets the list of index settings and index settings groups that shouldn't be restored from snapshot
     */
    public RestoreSnapshotRequest ignoreIndexSettings(List<String> ignoreIndexSettings) {
        this.ignoreIndexSettings = ignoreIndexSettings.toArray(new String[ignoreIndexSettings.size()]);
        return this;
    }

    /**
     * Returns the list of index settings and index settings groups that shouldn't be restored from snapshot
     */
    public String[] ignoreIndexSettings() {
        return ignoreIndexSettings;
    }

    /**
     * If set to true the restore procedure will restore aliases
     *
     * @param includeAliases true if aliases should be restored from the snapshot
     * @return this request
     */
    public RestoreSnapshotRequest includeAliases(boolean includeAliases) {
        this.includeAliases = includeAliases;
        return this;
    }

    /**
     * Returns true if aliases should be restored from this snapshot
     *
     * @return true if aliases should be restored
     */
    public boolean includeAliases() {
        return includeAliases;
    }

    /**
     * Sets settings that should be added/changed in all restored indices
     */
    public RestoreSnapshotRequest indexSettings(Settings settings) {
        this.indexSettings = settings;
        return this;
    }

    /**
     * Sets settings that should be added/changed in all restored indices
     */
    public RestoreSnapshotRequest indexSettings(Settings.Builder settings) {
        this.indexSettings = settings.build();
        return this;
    }

    /**
     * Sets settings that should be added/changed in all restored indices
     */
    public RestoreSnapshotRequest indexSettings(String source, XContentType xContentType) {
        this.indexSettings = Settings.builder().loadFromSource(source, xContentType).build();
        return this;
    }

    /**
     * Sets settings that should be added/changed in all restored indices
     */
    public RestoreSnapshotRequest indexSettings(Map<String, Object> source) {
        try {
            XContentBuilder builder = JsonXContent.builder();
            builder.map(source);
            indexSettings(Strings.toString(builder), builder.contentType());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
        return this;
    }

    /**
     * Returns settings that should be added/changed in all restored indices
     */
    public Settings indexSettings() {
        return this.indexSettings;
    }

    public RestoreSnapshotRequest includeIndices(boolean includeIndices) {
        this.includeIndices = includeIndices;
        return this;
    }

    public boolean includeIndices() {
        return includeIndices;
    }

    public RestoreSnapshotRequest includeCustomMetadata(boolean includeCustomMetadata) {
        this.includeCustomMetadata = includeCustomMetadata;
        return this;
    }

    public boolean includeCustomMetadata() {
        return includeCustomMetadata;
    }

    public RestoreSnapshotRequest customMetadataTypes(Set<String> types) {
        this.customMetadataTypes = types.toArray(new String[0]);
        return this;
    }

    public String[] customMetadataTypes() {
        return customMetadataTypes;
    }

    public RestoreSnapshotRequest includeGlobalSettings(boolean includeGlobalSettings) {
        this.includeGlobalSettings = includeGlobalSettings;
        return this;
    }

    public boolean includeGlobalSettings() {
        return includeGlobalSettings;
    }

    public RestoreSnapshotRequest globalSettings(List<String> globalSettings) {
        this.globalSettings = globalSettings.toArray(new String[0]);
        return this;
    }

    public String[] globalSettings() {
        return globalSettings;
    }

    public RestoreSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        snapshot = in.readString();
        repository = in.readString();
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        if (in.getVersion().before(Version.V_5_6_0)) {
            tableRenamePattern = in.readOptionalString();
            tableRenameReplacement = in.readOptionalString();
        } else {
            // Since 5.6 there are default values which behave similarly as pre-5.6 with NULL values.
            tableRenamePattern = in.readString();
            tableRenameReplacement = in.readString();
        }
        waitForCompletion = in.readBoolean();
        if (in.getVersion().before(Version.V_4_5_0)) {
            // ensure streaming BWC, read in unused `includeGlobalState`
            in.readBoolean();
        }
        partial = in.readBoolean();
        includeAliases = in.readBoolean();
        settings = readSettingsFromStream(in);
        indexSettings = readSettingsFromStream(in);
        ignoreIndexSettings = in.readStringArray();
        templates = in.readStringArray();
        if (in.getVersion().onOrAfter(Version.V_4_5_0)) {
            includeIndices = in.readBoolean();
            includeCustomMetadata = in.readBoolean();
            customMetadataTypes = in.readStringArray();
            includeGlobalSettings = in.readBoolean();
            globalSettings = in.readStringArray();
        }
        if (in.getVersion().onOrAfter(Version.V_5_6_0)) {
            tablesToRestore = in.readList(TableOrPartition::new);
            schemaRenamePattern = in.readString();
            schemaRenameReplacement = in.readString();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(snapshot);
        out.writeString(repository);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        if (out.getVersion().before(Version.V_5_6_0)) {
            out.writeOptionalString(tableRenamePattern);
            out.writeOptionalString(tableRenameReplacement);
        } else {
            // Since 5.6 there are default values which behave similarly as pre-5.6 with NULL values.
            out.writeString(tableRenamePattern);
            out.writeString(tableRenameReplacement);
        }
        out.writeBoolean(waitForCompletion);
        if (out.getVersion().before(Version.V_4_5_0)) {
            // streaming BWC, write remmoved `includeGlobalState`
            out.writeBoolean(false);
        }
        out.writeBoolean(partial);
        out.writeBoolean(includeAliases);
        writeSettingsToStream(out, settings);
        writeSettingsToStream(out, indexSettings);
        out.writeStringArray(ignoreIndexSettings);
        out.writeStringArray(templates);
        if (out.getVersion().onOrAfter(Version.V_4_5_0)) {
            out.writeBoolean(includeIndices);
            out.writeBoolean(includeCustomMetadata);
            out.writeStringArray(customMetadataTypes);
            out.writeBoolean(includeGlobalSettings);
            out.writeStringArray(globalSettings);
        }
        if (out.getVersion().onOrAfter(Version.V_5_6_0)) {
            out.writeCollection(tablesToRestore);
            out.writeString(schemaRenamePattern);
            out.writeString(schemaRenameReplacement);
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
        return waitForCompletion == that.waitForCompletion &&
            includeGlobalState == that.includeGlobalState &&
            partial == that.partial &&
            includeAliases == that.includeAliases &&
            Objects.equals(snapshot, that.snapshot) &&
            Objects.equals(repository, that.repository) &&
            Arrays.equals(indices, that.indices) &&
            Objects.equals(indicesOptions, that.indicesOptions) &&
            Objects.equals(tableRenamePattern, that.tableRenamePattern) &&
            Objects.equals(tableRenameReplacement, that.tableRenameReplacement) &&
            Objects.equals(schemaRenamePattern, that.schemaRenamePattern) &&
            Objects.equals(schemaRenameReplacement, that.schemaRenameReplacement) &&
            Objects.equals(settings, that.settings) &&
            Objects.equals(indexSettings, that.indexSettings) &&
            Arrays.equals(ignoreIndexSettings, that.ignoreIndexSettings) &&
            includeIndices == that.includeIndices &&
            includeCustomMetadata == that.includeCustomMetadata &&
            Arrays.equals(customMetadataTypes, that.customMetadataTypes) &&
            includeGlobalSettings == that.includeGlobalSettings &&
            Arrays.equals(globalSettings, that.globalSettings);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(snapshot, repository, indicesOptions, tableRenamePattern, tableRenameReplacement,waitForCompletion,
            includeGlobalState, partial, includeAliases, settings, indexSettings,
            includeIndices, includeCustomMetadata, includeGlobalSettings,
            schemaRenamePattern, schemaRenameReplacement
        );
        result = 31 * result + Arrays.hashCode(indices);
        result = 31 * result + Arrays.hashCode(ignoreIndexSettings);
        result = 31 * result + Arrays.hashCode(customMetadataTypes);
        result = 31 * result + Arrays.hashCode(globalSettings);
        return result;
    }
}
