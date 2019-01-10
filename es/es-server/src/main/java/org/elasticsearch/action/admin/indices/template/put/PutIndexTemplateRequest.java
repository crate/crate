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

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

/**
 * A request to create an index template.
 */
public class PutIndexTemplateRequest extends MasterNodeRequest<PutIndexTemplateRequest> implements IndicesRequest, ToXContent {

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(Loggers.getLogger(PutIndexTemplateRequest.class));

    private String name;

    private String cause = "";

    private List<String> indexPatterns;

    private int order;

    private boolean create;

    private Settings settings = EMPTY_SETTINGS;

    private Map<String, String> mappings = new HashMap<>();

    private final Set<Alias> aliases = new HashSet<>();

    private Integer version;

    public PutIndexTemplateRequest() {
    }

    /**
     * Constructs a new put index template request with the provided name.
     */
    public PutIndexTemplateRequest(String name) {
        this.name = name;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("name is missing", validationException);
        }
        if (indexPatterns == null || indexPatterns.size() == 0) {
            validationException = addValidationError("index patterns are missing", validationException);
        }
        return validationException;
    }

    /**
     * Sets the name of the index template.
     */
    public PutIndexTemplateRequest name(String name) {
        this.name = name;
        return this;
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

    public PutIndexTemplateRequest order(int order) {
        this.order = order;
        return this;
    }

    public int order() {
        return this.order;
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
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
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
     * @param type   The mapping type
     * @param source The mapping source
     * @param xContentType The type of content contained within the source
     */
    public PutIndexTemplateRequest mapping(String type, String source, XContentType xContentType) {
        return mapping(type, new BytesArray(source), xContentType);
    }

    /**
     * The cause for this index template creation.
     */
    public PutIndexTemplateRequest cause(String cause) {
        this.cause = cause;
        return this;
    }

    public String cause() {
        return this.cause;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     */
    public PutIndexTemplateRequest mapping(String type, XContentBuilder source) {
        return mapping(type, BytesReference.bytes(source), source.contentType());
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     * @param xContentType the source content type
     */
    public PutIndexTemplateRequest mapping(String type, BytesReference source, XContentType xContentType) {
        Objects.requireNonNull(xContentType);
        try {
            mappings.put(type, XContentHelper.convertToJson(source, false, false, xContentType));
            return this;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to convert source to json", e);
        }
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     */
    public PutIndexTemplateRequest mapping(String type, Map<String, Object> source) {
        // wrap it in a type map if its not
        if (source.size() != 1 || !source.containsKey(type)) {
            source = MapBuilder.<String, Object>newMapBuilder().put(type, source).map();
        }
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(source);
            return mapping(type, builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * A specialized simplified mapping source method, takes the form of simple properties definition:
     * ("field1", "type=string,store=true").
     */
    public PutIndexTemplateRequest mapping(String type, Object... source) {
        mapping(type, PutMappingRequest.buildFromSimplifiedDef(type, source));
        return this;
    }

    public Map<String, String> mappings() {
        return this.mappings;
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequest source(XContentBuilder templateBuilder) {
        try {
            return source(BytesReference.bytes(templateBuilder), templateBuilder.contentType());
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to build json for template request", e);
        }
    }

    /**
     * The template source definition.
     */
    @SuppressWarnings("unchecked")
    public PutIndexTemplateRequest source(Map templateSource) {
        Map<String, Object> source = templateSource;
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String name = entry.getKey();
            if (name.equals("template")) {
                // This is needed to allow for bwc (beats, logstash) with pre-5.0 templates (#21009)
                if(entry.getValue() instanceof String) {
                    DEPRECATION_LOGGER.deprecated("Deprecated field [template] used, replaced by [index_patterns]");
                    patterns(Collections.singletonList((String) entry.getValue()));
                }
            } else if (name.equals("index_patterns")) {
                if(entry.getValue() instanceof String) {
                    patterns(Collections.singletonList((String) entry.getValue()));
                } else if (entry.getValue() instanceof List) {
                    List<String> elements = ((List<?>) entry.getValue()).stream().map(Object::toString).collect(Collectors.toList());
                    patterns(elements);
                } else {
                    throw new IllegalArgumentException("Malformed [template] value, should be a string or a list of strings");
                }
            } else if (name.equals("order")) {
                order(XContentMapValues.nodeIntegerValue(entry.getValue(), order()));
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
                    mapping(entry1.getKey(), (Map<String, Object>) entry1.getValue());
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
    public PutIndexTemplateRequest source(String templateSource, XContentType xContentType) {
        return source(XContentHelper.convertToMap(xContentType.xContent(), templateSource, true));
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequest source(byte[] source, XContentType xContentType) {
        return source(source, 0, source.length, xContentType);
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequest source(byte[] source, int offset, int length, XContentType xContentType) {
        return source(new BytesArray(source, offset, length), xContentType);
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequest source(BytesReference source, XContentType xContentType) {
        return source(XContentHelper.convertToMap(source, true, xContentType).v2());
    }

    public Set<Alias> aliases() {
        return this.aliases;
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    @SuppressWarnings("unchecked")
    public PutIndexTemplateRequest aliases(Map source) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(source);
            return aliases(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public PutIndexTemplateRequest aliases(XContentBuilder source) {
        return aliases(BytesReference.bytes(source));
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public PutIndexTemplateRequest aliases(String source) {
        return aliases(new BytesArray(source));
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public PutIndexTemplateRequest aliases(BytesReference source) {
        // EMPTY is safe here because we never call namedObject
        try (XContentParser parser = XContentHelper
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, source)) {
            //move to the first alias
            parser.nextToken();
            while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                alias(Alias.fromXContent(parser));
            }
            return this;
        } catch(IOException e) {
            throw new ElasticsearchParseException("Failed to parse aliases", e);
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

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        cause = in.readString();
        name = in.readString();

        if (in.getVersion().onOrAfter(Version.V_6_0_0_alpha1)) {
            indexPatterns = in.readList(StreamInput::readString);
        } else {
            indexPatterns = Collections.singletonList(in.readString());
        }
        order = in.readInt();
        create = in.readBoolean();
        settings = readSettingsFromStream(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            final String type = in.readString();
            String mappingSource = in.readString();
            if (in.getVersion().before(Version.V_5_3_0)) {
                // we do not know the incoming type so convert it if needed
                mappingSource =
                    XContentHelper.convertToJson(new BytesArray(mappingSource), false, false, XContentFactory.xContentType(mappingSource));
            }
            mappings.put(type, mappingSource);
        }
        if (in.getVersion().before(Version.V_6_5_0)) {
            // Used to be used for custom index metadata
            int customSize = in.readVInt();
            assert customSize == 0 : "expected not to have any custom metadata";
            if (customSize > 0) {
                throw new IllegalStateException("unexpected custom metadata when none is supported");
            }
        }
        int aliasesSize = in.readVInt();
        for (int i = 0; i < aliasesSize; i++) {
            aliases.add(Alias.read(in));
        }
        version = in.readOptionalVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(cause);
        out.writeString(name);
        if (out.getVersion().onOrAfter(Version.V_6_0_0_alpha1)) {
            out.writeStringList(indexPatterns);
        } else {
            out.writeString(indexPatterns.size() > 0 ? indexPatterns.get(0) : "");
        }
        out.writeInt(order);
        out.writeBoolean(create);
        writeSettingsToStream(settings, out);
        out.writeVInt(mappings.size());
        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
        if (out.getVersion().before(Version.V_6_5_0)) {
            out.writeVInt(0);
        }
        out.writeVInt(aliases.size());
        for (Alias alias : aliases) {
            alias.writeTo(out);
        }
        out.writeOptionalVInt(version);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("index_patterns", indexPatterns);
        builder.field("order", order);
        if (version != null) {
            builder.field("version", version);
        }

        builder.startObject("settings");
        settings.toXContent(builder, params);
        builder.endObject();

        builder.startObject("mappings");
        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            builder.field(entry.getKey());
            XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, entry.getValue());
            builder.copyCurrentStructure(parser);
        }
        builder.endObject();

        builder.startObject("aliases");
        for (Alias alias : aliases) {
            alias.toXContent(builder, params);
        }
        builder.endObject();

        return builder;
    }
}
