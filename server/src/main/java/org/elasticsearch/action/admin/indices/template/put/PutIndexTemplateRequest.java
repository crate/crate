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

package org.elasticsearch.action.admin.indices.template.put;

import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;
import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import io.crate.Constants;
import io.crate.server.xcontent.LoggingDeprecationHandler;
import io.crate.server.xcontent.XContentHelper;

/**
 * A request to create an index template.
 */
public class PutIndexTemplateRequest extends MasterNodeRequest<PutIndexTemplateRequest> implements IndicesRequest {

    private final String name;

    private List<String> indexPatterns;

    private boolean create;

    private Settings settings = EMPTY_SETTINGS;

    private String mapping;

    private final Set<Alias> aliases = new HashSet<>();

    private Integer version;

    /**
     * Constructs a new put index template request with the provided name.
     */
    public PutIndexTemplateRequest(String name) {
        this.name = name;
    }

    /**
     * The name of the index template.
     */
    public String name() {
        return this.name;
    }

    public PutIndexTemplateRequest patterns(List<String> indexPatterns) {
        this.indexPatterns = indexPatterns;
        return this;
    }

    public List<String> patterns() {
        return this.indexPatterns;
    }

    public PutIndexTemplateRequest version(Integer version) {
        this.version = version;
        return this;
    }

    public Integer version() {
        return this.version;
    }

    /**
     * Set to {@code true} to force only creation, not an update of an index template. If it already
     * exists, it will fail with an {@link IllegalArgumentException}.
     */
    public PutIndexTemplateRequest create(boolean create) {
        this.create = create;
        return this;
    }

    public boolean create() {
        return create;
    }

    /**
     * The settings to create the index template with.
     */
    public PutIndexTemplateRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * The settings to create the index template with.
     */
    public PutIndexTemplateRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * The settings to create the index template with (either json/yaml format).
     */
    public PutIndexTemplateRequest settings(String source, XContentType xContentType) {
        this.settings = Settings.builder().loadFromSource(source, xContentType).build();
        return this;
    }

    /**
     * The settings to create the index template with (either json or yaml format).
     */
    public PutIndexTemplateRequest settings(Map<String, Object> source) {
        try {
            XContentBuilder builder = JsonXContent.builder();
            builder.map(source);
            settings(Strings.toString(builder), XContentType.JSON);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
        return this;
    }

    public Settings settings() {
        return this.settings;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param source The mapping source
     */
    public PutIndexTemplateRequest mapping(Map<String, Object> source) {
        // wrap it in a type map if its not
        if (source.size() != 1 || !source.containsKey(Constants.DEFAULT_MAPPING_TYPE)) {
            source = Map.of(Constants.DEFAULT_MAPPING_TYPE, source);
        }
        try {
            XContentBuilder builder = JsonXContent.builder();
            builder.map(source);
            mapping = BytesReference.bytes(builder).utf8ToString();
            return this;
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    public String mapping() {
        return this.mapping;
    }

    /**
     * The template source definition.
     */
    @SuppressWarnings("unchecked")
    public PutIndexTemplateRequest source(Map<String, Object> source) {
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String name = entry.getKey();
            if (name.equals("index_patterns")) {
                if (entry.getValue() instanceof String) {
                    patterns(Collections.singletonList((String) entry.getValue()));
                } else if (entry.getValue() instanceof List) {
                    List<String> elements = ((List<?>) entry.getValue()).stream().map(Object::toString).collect(Collectors.toList());
                    patterns(elements);
                } else {
                    throw new IllegalArgumentException("Malformed [template] value, should be a string or a list of strings");
                }
            } else if ("version".equals(name)) {
                if ((entry.getValue() instanceof Integer) == false) {
                    throw new IllegalArgumentException("Malformed [version] value, should be an integer");
                }
                version((Integer)entry.getValue());
            } else if (name.equals("settings")) {
                if ((entry.getValue() instanceof Map) == false) {
                    throw new IllegalArgumentException("Malformed [settings] section, should include an inner object");
                }
                settings((Map<String, Object>) entry.getValue());
            } else if (name.equals("mappings")) {
                Map<String, Object> mappings = (Map<String, Object>) entry.getValue();
                for (Map.Entry<String, Object> entry1 : mappings.entrySet()) {
                    if (!(entry1.getValue() instanceof Map)) {
                        throw new IllegalArgumentException(
                            "Malformed [mappings] section for type [" + entry1.getKey() +
                                "], should include an inner object describing the mapping");
                    }
                    mapping((Map<String, Object>) entry1.getValue());
                }
            } else if (name.equals("aliases")) {
                aliases((Map<String, Object>) entry.getValue());
            } else {
                throw new ElasticsearchParseException("unknown key [{}] in the template ", name);
            }
        }
        return this;
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequest source(BytesReference source, XContentType xContentType) {
        return source(XContentHelper.convertToMap(source, true, xContentType).map());
    }

    public Set<Alias> aliases() {
        return this.aliases;
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public PutIndexTemplateRequest aliases(Map<String, Object> source) {
        try {
            XContentBuilder builder = JsonXContent.builder();
            builder.map(source);
            // EMPTY is safe here because we never call namedObject
            try (XContentParser parser = XContentHelper
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder), XContentType.JSON)) {
                //move to the first alias
                parser.nextToken();
                while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    alias(Alias.fromXContent(parser));
                }
                return this;
            } catch (IOException e) {
                throw new ElasticsearchParseException("Failed to parse aliases", e);
            }
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Adds an alias that will be added when the index gets created.
     *
     * @param alias   The metadata for the new alias
     * @return  the index template creation request
     */
    public PutIndexTemplateRequest alias(Alias alias) {
        aliases.add(alias);
        return this;
    }

    @Override
    public String[] indices() {
        return indexPatterns.toArray(new String[indexPatterns.size()]);
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictExpand();
    }

    public PutIndexTemplateRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().before(Version.V_5_2_0)) {
            in.readString(); // cause
        }
        name = in.readString();

        indexPatterns = in.readList(StreamInput::readString);
        if (in.getVersion().before(Version.V_5_4_0)) {
            in.readInt(); // order
        }
        create = in.readBoolean();
        settings = readSettingsFromStream(in);
        if (in.getVersion().onOrAfter(Version.V_5_2_0)) {
            mapping = in.readString();
        } else {
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                in.readString(); // always "default"
                String mappingSource = in.readString();
                mapping = mappingSource;
            }
        }
        int aliasesSize = in.readVInt();
        for (int i = 0; i < aliasesSize; i++) {
            aliases.add(new Alias(in));
        }
        version = in.readOptionalVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_5_2_0)) {
            // cause
            out.writeString("");
        }
        out.writeString(name);
        out.writeStringCollection(indexPatterns);
        if (out.getVersion().before(Version.V_5_4_0)) {
            // Send some dummy value.
            // Order is supposed to be a tie breaker for multiple templates matching a single pattern.
            // We always have only 1 matching template per pattern, ordering is not needed.
            out.writeInt(1);
        }
        out.writeBoolean(create);
        writeSettingsToStream(out, settings);
        if (out.getVersion().onOrAfter(Version.V_5_2_0)) {
            out.writeString(mapping);
        } else {
            out.writeVInt(1);
            out.writeString(Constants.DEFAULT_MAPPING_TYPE);
            out.writeString(mapping);
        }
        out.writeVInt(aliases.size());
        for (Alias alias : aliases) {
            alias.writeTo(out);
        }
        out.writeOptionalVInt(version);
    }
}
