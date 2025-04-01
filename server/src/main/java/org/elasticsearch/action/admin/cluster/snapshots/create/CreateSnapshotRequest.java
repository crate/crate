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

package org.elasticsearch.action.admin.cluster.snapshots.create;

import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import io.crate.metadata.IndexName;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

/**
 * Create snapshot request
 * <p>
 * The only mandatory parameter is repository name. The repository name has to satisfy the following requirements
 * <ul>
 * <li>be a non-empty string</li>
 * <li>must not contain whitespace (tabs or spaces)</li>
 * <li>must not contain comma (',')</li>
 * <li>must not contain hash sign ('#')</li>
 * <li>must not start with underscore ('-')</li>
 * <li>must be lowercase</li>
 * <li>must not contain invalid file name characters {@link org.elasticsearch.common.Strings#INVALID_FILENAME_CHARS} </li>
 * </ul>
 */
public class CreateSnapshotRequest extends MasterNodeRequest<CreateSnapshotRequest> {

    private String snapshot;

    private String repository;

    private List<RelationName> relationNames = List.of();

    private List<PartitionName> partitionNames = List.of();

    private boolean partial = false;

    private Settings settings = Settings.EMPTY;

    private boolean includeGlobalState = true;

    private boolean waitForCompletion;

    /**
     * Constructs a new put repository request with the provided snapshot and repository names
     *
     * @param repository repository name
     * @param snapshot   snapshot name
     */
    public CreateSnapshotRequest(String repository, String snapshot) {
        this.snapshot = snapshot;
        this.repository = repository;
    }

    public CreateSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        snapshot = in.readString();
        repository = in.readString();
        if (in.getVersion().before(Version.V_6_0_0)) {
            String[] oldIndices = in.readStringArray();
            IndicesOptions.readIndicesOptions(in);
            String[] oldTemplates = in.readStringArray();
            HashSet<RelationName> relationNames = new HashSet<>();
            ArrayList<PartitionName> partitionNames = new ArrayList<>();
            for (String index : oldIndices) {
                IndexParts indexParts = IndexName.decode(index);
                if (indexParts.isPartitioned()) {
                    partitionNames.add(PartitionName.fromIndexOrTemplate(index));
                } else {
                    relationNames.add(indexParts.toRelationName());
                }
            }
            for (String template : oldTemplates) {
                relationNames.add(IndexName.decode(template).toRelationName());
            }
            this.relationNames = new ArrayList<>(relationNames);
            this.partitionNames = partitionNames;
        }
        settings = readSettingsFromStream(in);
        includeGlobalState = in.readBoolean();
        waitForCompletion = in.readBoolean();
        partial = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_6_0_0)) {
            relationNames = in.readList(RelationName::new);
            partitionNames = in.readList(PartitionName::new);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(snapshot);
        out.writeString(repository);
        if (out.getVersion().before(Version.V_6_0_0)) {
            // old indices
            HashSet<String> indices = new HashSet<>();
            relationNames.stream().map(RelationName::indexNameOrAlias).forEach(indices::add);
            partitionNames.stream().map(PartitionName::asIndexName).forEach(indices::add);
            out.writeStringArray(indices.toArray(String[]::new));
            IndicesOptions.STRICT_EXPAND_OPEN.writeIndicesOptions(out);

            // old templates, we don't know if the relations are partitioned or not, so we always add templates for
            out.writeStringArray(
                relationNames.stream()
                    .map(r -> PartitionName.templateName(r.schema(), r.name()))
                    .toArray(String[]::new)
            );
        }
        writeSettingsToStream(out, settings);
        out.writeBoolean(includeGlobalState);
        out.writeBoolean(waitForCompletion);
        out.writeBoolean(partial);
        if (out.getVersion().onOrAfter(Version.V_6_0_0)) {
            out.writeList(relationNames);
            out.writeList(partitionNames);
        }
    }

    /**
     * Sets the snapshot name
     *
     * @param snapshot snapshot name
     */
    public CreateSnapshotRequest snapshot(String snapshot) {
        this.snapshot = snapshot;
        return this;
    }

    /**
     * The snapshot name
     *
     * @return snapshot name
     */
    public String snapshot() {
        return this.snapshot;
    }

    /**
     * Sets repository name
     *
     * @param repository name
     * @return this request
     */
    public CreateSnapshotRequest repository(String repository) {
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

    public CreateSnapshotRequest relationNames(List<RelationName> relationNames) {
        this.relationNames = relationNames;
        return this;
    }

    public List<RelationName> relationNames() {
        return relationNames;
    }

    public CreateSnapshotRequest partitionNames(List<PartitionName> partitionNames) {
        this.partitionNames = partitionNames;
        return this;
    }

    public List<PartitionName> partitionNames() {
        return partitionNames;
    }

    /**
     * Returns true if indices with unavailable shards should be be partially snapshotted.
     *
     * @return the desired behaviour regarding indices options
     */
    public boolean partial() {
        return partial;
    }

    /**
     * Set to true to allow indices with unavailable shards to be partially snapshotted.
     *
     * @param partial true if indices with unavailable shards should be be partially snapshotted.
     * @return this request
     */
    public CreateSnapshotRequest partial(boolean partial) {
        this.partial = partial;
        return this;
    }

    /**
     * If set to true the operation should wait for the snapshot completion before returning.
     *
     * By default, the operation will return as soon as snapshot is initialized. It can be changed by setting this
     * flag to true.
     *
     * @param waitForCompletion true if operation should wait for the snapshot completion
     * @return this request
     */
    public CreateSnapshotRequest waitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
        return this;
    }

    /**
     * Returns true if the request should wait for the snapshot completion before returning
     *
     * @return true if the request should wait for completion
     */
    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    /**
     * Sets repository-specific snapshot settings.
     * <p>
     * See repository documentation for more information.
     *
     * @param settings repository-specific snapshot settings
     * @return this request
     */
    public CreateSnapshotRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * Sets repository-specific snapshot settings.
     * <p>
     * See repository documentation for more information.
     *
     * @param settings repository-specific snapshot settings
     * @return this request
     */
    public CreateSnapshotRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * Sets repository-specific snapshot settings in JSON or YAML format
     * <p>
     * See repository documentation for more information.
     *
     * @param source repository-specific snapshot settings
     * @param xContentType the content type of the source
     * @return this request
     */
    public CreateSnapshotRequest settings(String source, XContentType xContentType) {
        this.settings = Settings.builder().loadFromSource(source, xContentType).build();
        return this;
    }

    /**
     * Sets repository-specific snapshot settings.
     * <p>
     * See repository documentation for more information.
     *
     * @param source repository-specific snapshot settings
     * @return this request
     */
    public CreateSnapshotRequest settings(Map<String, Object> source) {
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
     * Returns repository-specific snapshot settings
     *
     * @return repository-specific snapshot settings
     */
    public Settings settings() {
        return this.settings;
    }

    /**
     * Set to true if global state should be stored as part of the snapshot
     *
     * @param includeGlobalState true if global state should be stored
     * @return this request
     */
    public CreateSnapshotRequest includeGlobalState(boolean includeGlobalState) {
        this.includeGlobalState = includeGlobalState;
        return this;
    }

    /**
     * Returns true if global state should be stored as part of the snapshot
     *
     * @return true if global state should be stored as part of the snapshot
     */
    public boolean includeGlobalState() {
        return includeGlobalState;
    }

    @Override
    public String getDescription() {
        return "snapshot [" + repository + ":" + snapshot + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateSnapshotRequest that = (CreateSnapshotRequest) o;
        return partial == that.partial &&
            includeGlobalState == that.includeGlobalState &&
            waitForCompletion == that.waitForCompletion &&
            Objects.equals(snapshot, that.snapshot) &&
            Objects.equals(repository, that.repository) &&
            Objects.equals(relationNames, that.relationNames) &&
            Objects.equals(partitionNames, that.partitionNames) &&
            Objects.equals(settings, that.settings) &&
            Objects.equals(masterNodeTimeout, that.masterNodeTimeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            snapshot,
            repository,
            relationNames,
            partitionNames,
            partial,
            settings,
            includeGlobalState,
            waitForCompletion
        );
    }

    @Override
    public String toString() {
        return "CreateSnapshotRequest{" +
            "snapshot='" + snapshot + '\'' +
            ", repository='" + repository + '\'' +
            ", relationNames=" + relationNames +
            ", partitionNames=" + partitionNames +
            ", partial=" + partial +
            ", settings=" + settings +
            ", includeGlobalState=" + includeGlobalState +
            ", waitForCompletion=" + waitForCompletion +
            ", masterNodeTimeout=" + masterNodeTimeout +
            '}';
    }
}
