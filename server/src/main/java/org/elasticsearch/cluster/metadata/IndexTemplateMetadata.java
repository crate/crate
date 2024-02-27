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

package org.elasticsearch.cluster.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import io.crate.Constants;
import io.crate.common.collections.MapBuilder;
import io.crate.server.xcontent.XContentHelper;

public class IndexTemplateMetadata extends AbstractDiffable<IndexTemplateMetadata> {

    private final String name;

    /**
     * The version is an arbitrary number managed by the user so that they can easily and quickly verify the existence of a given template.
     * Expected usage:
     * <pre><code>
     * PUT /_template/my_template
     * {
     *   "index_patterns": ["my_index-*"],
     *   "mappings": { ... },
     *   "version": 1
     * }
     * </code></pre>
     * Then, some process from the user can occasionally verify that the template exists with the appropriate version without having to
     * check the template's content:
     * <pre><code>
     * GET /_template/my_template?filter_path=*.version
     * </code></pre>
     */
    @Nullable
    private final Integer version;

    private final List<String> patterns;

    private final Settings settings;

    private final CompressedXContent mapping;

    private final ImmutableOpenMap<String, AliasMetadata> aliases;

    public IndexTemplateMetadata(String name, Integer version,
                                 List<String> patterns, Settings settings,
                                 CompressedXContent mapping,
                                 ImmutableOpenMap<String, AliasMetadata> aliases) {
        if (patterns == null || patterns.isEmpty()) {
            throw new IllegalArgumentException("Index patterns must not be null or empty; got " + patterns);
        }
        if (mapping == null) {
            throw new IllegalArgumentException("Template must have a mapping");
        }
        this.name = name;
        this.version = version;
        this.patterns = patterns;
        this.settings = settings;
        this.mapping = mapping;
        this.aliases = aliases;
    }

    public String name() {
        return this.name;
    }

    @Nullable
    public Integer getVersion() {
        return version();
    }

    @Nullable
    public Integer version() {
        return version;
    }

    public String getName() {
        return this.name;
    }

    public List<String> patterns() {
        return this.patterns;
    }

    public List<String> getPatterns() {
        return this.patterns;
    }

    public Settings settings() {
        return this.settings;
    }

    public Settings getSettings() {
        return settings();
    }

    public CompressedXContent mapping() {
        return this.mapping;
    }

    public ImmutableOpenMap<String, AliasMetadata> aliases() {
        return this.aliases;
    }

    public ImmutableOpenMap<String, AliasMetadata> getAliases() {
        return this.aliases;
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexTemplateMetadata that = (IndexTemplateMetadata) o;

        if (!mapping.equals(that.mapping)) return false;
        if (!name.equals(that.name)) return false;
        if (!settings.equals(that.settings)) return false;
        if (!patterns.equals(that.patterns)) return false;

        return Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + Objects.hashCode(version);
        result = 31 * result + patterns.hashCode();
        result = 31 * result + settings.hashCode();
        result = 31 * result + mapping.hashCode();
        return result;
    }

    public static IndexTemplateMetadata readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder(in.readString());
        if (in.getVersion().before(Version.V_5_4_0)) {
            in.readInt(); // order
        }
        builder.patterns(in.readList(StreamInput::readString));
        builder.settings(Settings.readSettingsFromStream(in));
        if (in.getVersion().onOrAfter(Version.V_5_2_0)) {
            builder.putMapping(CompressedXContent.readCompressedString(in));
        } else {
            int mappingsSize = in.readVInt();
            assert mappingsSize <= 1 : "There was always a single `default` mapping";
            for (int i = 0; i < mappingsSize; i++) {
                in.readString(); // mappingType, was always "default"
                builder.putMapping(CompressedXContent.readCompressedString(in));
            }
        }
        int aliasesSize = in.readVInt();
        for (int i = 0; i < aliasesSize; i++) {
            AliasMetadata aliasMd = new AliasMetadata(in);
            builder.putAlias(aliasMd);
        }
        builder.version(in.readOptionalVInt());
        return builder.build();
    }

    public static Diff<IndexTemplateMetadata> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(IndexTemplateMetadata::readFrom, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        if (out.getVersion().before(Version.V_5_4_0)) {
            // Send some dummy value.
            // Order is supposed to be a tie breaker for multiple templates matching a single pattern.
            // We always have only 1 matching template per pattern, ordering is not needed.
            out.writeInt(1);
        }
        out.writeStringCollection(patterns);
        Settings.writeSettingsToStream(out, settings);
        if (out.getVersion().onOrAfter(Version.V_5_2_0)) {
            mapping.writeTo(out);
        } else {
            out.writeVInt(1);
            out.writeString(Constants.DEFAULT_MAPPING_TYPE);
            mapping.writeTo(out);
        }
        out.writeVInt(aliases.size());
        for (ObjectCursor<AliasMetadata> cursor : aliases.values()) {
            cursor.value.writeTo(out);
        }
        out.writeOptionalVInt(version);
    }

    public static class Builder {

        private static final Set<String> VALID_FIELDS = Set.of(
            "template", "mappings", "settings", "index_patterns", "aliases", "version");

        private String name;

        private Integer version;

        private List<String> indexPatterns;

        private Settings settings = Settings.Builder.EMPTY_SETTINGS;

        private CompressedXContent mapping;

        private final ImmutableOpenMap.Builder<String, AliasMetadata> aliases;

        public Builder(String name) {
            this.name = name;
            aliases = ImmutableOpenMap.builder();
        }

        public Builder(IndexTemplateMetadata indexTemplateMetadata) {
            this.name = indexTemplateMetadata.name();
            version(indexTemplateMetadata.version());
            patterns(indexTemplateMetadata.patterns());
            settings(indexTemplateMetadata.settings());

            mapping = indexTemplateMetadata.mapping();
            aliases = ImmutableOpenMap.builder(indexTemplateMetadata.aliases());
        }

        public Builder version(Integer version) {
            this.version = version;
            return this;
        }

        public Builder patterns(List<String> indexPatterns) {
            this.indexPatterns = indexPatterns;
            return this;
        }


        public Builder settings(Settings.Builder settings) {
            this.settings = settings.build();
            return this;
        }

        public Builder settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public Builder putMapping(CompressedXContent mappingSource) {
            if (mappingSource != null) {
                mapping = mappingSource;
            }
            return this;
        }

        public Builder putMapping(String mappingSource) throws IOException {
            mapping = new CompressedXContent(mappingSource);
            return this;
        }

        public Builder putAlias(AliasMetadata aliasMetadata) {
            aliases.put(aliasMetadata.alias(), aliasMetadata);
            return this;
        }

        public Builder putAlias(AliasMetadata.Builder aliasMetadata) {
            aliases.put(aliasMetadata.alias(), aliasMetadata.build());
            return this;
        }

        public IndexTemplateMetadata build() {
            return new IndexTemplateMetadata(name, version, indexPatterns, settings, mapping, aliases.build());
        }

        public static void toXContent(IndexTemplateMetadata indexTemplateMetadata, XContentBuilder builder, ToXContent.Params params)
                throws IOException {
            builder.startObject(indexTemplateMetadata.name());

            toInnerXContent(indexTemplateMetadata, builder, params);

            builder.endObject();
        }

        public static void toInnerXContent(IndexTemplateMetadata indexTemplateMetadata, XContentBuilder builder, ToXContent.Params params)
            throws IOException {

            if (indexTemplateMetadata.version() != null) {
                builder.field("version", indexTemplateMetadata.version());
            }
            builder.field("index_patterns", indexTemplateMetadata.patterns());

            builder.startObject("settings");
            indexTemplateMetadata.settings().toXContent(builder, params);
            builder.endObject();

            if (params.paramAsBoolean("reduce_mappings", false)) {
                builder.startObject("mappings");
                Map<String, Object> mapping = XContentHelper.convertToMap(indexTemplateMetadata.mapping().uncompressed(), true, XContentType.JSON).map();
                if (mapping.size() == 1 && mapping.containsKey(Constants.DEFAULT_MAPPING_TYPE)) {
                    // the type name is the root value, reduce it
                    mapping = (Map<String, Object>) mapping.get(Constants.DEFAULT_MAPPING_TYPE);
                }
                builder.field(Constants.DEFAULT_MAPPING_TYPE);
                builder.map(mapping);
                builder.endObject();
            } else {
                builder.startArray("mappings");
                Map<String, Object> mapping = XContentHelper.convertToMap(indexTemplateMetadata.mapping().uncompressed(), true, XContentType.JSON).map();
                builder.map(mapping);
                builder.endArray();
            }

            builder.startObject("aliases");
            for (ObjectCursor<AliasMetadata> cursor : indexTemplateMetadata.aliases().values()) {
                AliasMetadata.Builder.toXContent(cursor.value, builder, params);
            }
            builder.endObject();
        }

        public static IndexTemplateMetadata fromXContent(XContentParser parser, String templateName) throws IOException {
            Builder builder = new Builder(templateName);

            String currentFieldName = skipTemplateName(parser);
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("settings".equals(currentFieldName)) {
                        Settings.Builder templateSettingsBuilder = Settings.builder();
                        templateSettingsBuilder.put(Settings.fromXContent(parser));
                        templateSettingsBuilder.normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX);
                        builder.settings(templateSettingsBuilder.build());
                    } else if ("mappings".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                String mappingType = currentFieldName;
                                Map<String, Object> mappingSource =
                                    MapBuilder.<String, Object>newMapBuilder().put(mappingType, parser.mapOrdered()).map();
                                builder.putMapping(Strings.toString(JsonXContent.builder().map(mappingSource)));
                            }
                        }
                    } else if ("aliases".equals(currentFieldName)) {
                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            builder.putAlias(AliasMetadata.Builder.fromXContent(parser));
                        }
                    } else {
                        throw new ElasticsearchParseException("unknown key [{}] for index template", currentFieldName);
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("mappings".equals(currentFieldName)) {
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            Map<String, Object> mapping = parser.mapOrdered();
                            if (mapping.size() == 1) {
                                String mappingSource = Strings.toString(JsonXContent.builder().map(mapping));
                                if (mappingSource == null) {
                                    // crap, no mapping source, warn?
                                } else {
                                    builder.putMapping(mappingSource);
                                }
                            }
                        }
                    } else if ("index_patterns".equals(currentFieldName)) {
                        List<String> index_patterns = new ArrayList<>();
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            index_patterns.add(parser.text());
                        }
                        builder.patterns(index_patterns);
                    }
                } else if (token.isValue()) {
                    if ("version".equals(currentFieldName)) {
                        builder.version(parser.intValue());
                    }
                }
            }
            return builder.build();
        }

        private static String skipTemplateName(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.nextToken();
            if (token != null && token == XContentParser.Token.START_OBJECT) {
                token = parser.nextToken();
                if (token == XContentParser.Token.FIELD_NAME) {
                    String currentFieldName = parser.currentName();
                    if (VALID_FIELDS.contains(currentFieldName)) {
                        return currentFieldName;
                    } else {
                        // we just hit the template name, which should be ignored and we move on
                        parser.nextToken();
                    }
                }
            }

            return null;
        }
    }

}
