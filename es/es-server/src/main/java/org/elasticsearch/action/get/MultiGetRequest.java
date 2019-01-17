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

package org.elasticsearch.action.get;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.RealtimeRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

public class MultiGetRequest extends ActionRequest
        implements Iterable<MultiGetRequest.Item>, CompositeIndicesRequest, RealtimeRequest, ToXContentObject {

    private static final ParseField DOCS = new ParseField("docs");
    private static final ParseField INDEX = new ParseField("_index");
    private static final ParseField TYPE = new ParseField("_type");
    private static final ParseField ID = new ParseField("_id");
    private static final ParseField ROUTING = new ParseField("routing", "_routing");
    private static final ParseField PARENT = new ParseField("parent", "_parent");
    private static final ParseField VERSION = new ParseField("version", "_version");
    private static final ParseField VERSION_TYPE = new ParseField("version_type", "_version_type", "_versionType", "versionType");
    private static final ParseField FIELDS = new ParseField("fields");
    private static final ParseField STORED_FIELDS = new ParseField("stored_fields");
    private static final ParseField SOURCE = new ParseField("_source");

    /**
     * A single get item.
     */
    public static class Item implements Streamable, IndicesRequest, ToXContentObject {

        private String index;
        private String type;
        private String id;
        private String routing;
        private String parent;
        private String[] storedFields;
        private long version = Versions.MATCH_ANY;
        private VersionType versionType = VersionType.INTERNAL;
        private FetchSourceContext fetchSourceContext;

        public Item() {

        }

        /**
         * Constructs a single get item.
         *
         * @param index The index name
         * @param type  The type (can be null)
         * @param id    The id
         */
        public Item(String index, @Nullable String type, String id) {
            this.index = index;
            this.type = type;
            this.id = id;
        }

        public String index() {
            return this.index;
        }

        @Override
        public String[] indices() {
            return new String[]{index};
        }

        @Override
        public IndicesOptions indicesOptions() {
            return GetRequest.INDICES_OPTIONS;
        }

        public Item index(String index) {
            this.index = index;
            return this;
        }

        public String type() {
            return this.type;
        }

        public Item type(String type) {
            this.type = type;
            return this;
        }

        public String id() {
            return this.id;
        }

        /**
         * The routing associated with this document.
         */
        public Item routing(String routing) {
            this.routing = routing;
            return this;
        }

        public String routing() {
            return this.routing;
        }

        public Item parent(String parent) {
            this.parent = parent;
            return this;
        }

        /**
         * @return The parent for this request.
         */
        public String parent() {
            return parent;
        }

        public Item storedFields(String... fields) {
            this.storedFields = fields;
            return this;
        }

        public String[] storedFields() {
            return this.storedFields;
        }

        public long version() {
            return version;
        }

        public Item version(long version) {
            this.version = version;
            return this;
        }

        public VersionType versionType() {
            return versionType;
        }

        public Item versionType(VersionType versionType) {
            this.versionType = versionType;
            return this;
        }

        public FetchSourceContext fetchSourceContext() {
            return this.fetchSourceContext;
        }

        /**
         * Allows setting the {@link FetchSourceContext} for this request, controlling if and how _source should be returned.
         */
        public Item fetchSourceContext(FetchSourceContext fetchSourceContext) {
            this.fetchSourceContext = fetchSourceContext;
            return this;
        }

        public static Item readItem(StreamInput in) throws IOException {
            Item item = new Item();
            item.readFrom(in);
            return item;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            index = in.readString();
            type = in.readOptionalString();
            id = in.readString();
            routing = in.readOptionalString();
            parent = in.readOptionalString();
            storedFields = in.readOptionalStringArray();
            version = in.readLong();
            versionType = VersionType.fromValue(in.readByte());

            fetchSourceContext = in.readOptionalWriteable(FetchSourceContext::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeOptionalString(type);
            out.writeString(id);
            out.writeOptionalString(routing);
            out.writeOptionalString(parent);
            out.writeOptionalStringArray(storedFields);
            out.writeLong(version);
            out.writeByte(versionType.getValue());

            out.writeOptionalWriteable(fetchSourceContext);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(INDEX.getPreferredName(), index);
            builder.field(TYPE.getPreferredName(), type);
            builder.field(ID.getPreferredName(), id);
            builder.field(ROUTING.getPreferredName(), routing);
            builder.field(PARENT.getPreferredName(), parent);
            builder.field(STORED_FIELDS.getPreferredName(), storedFields);
            builder.field(VERSION.getPreferredName(), version);
            builder.field(VERSION_TYPE.getPreferredName(), VersionType.toString(versionType));
            builder.field(SOURCE.getPreferredName(), fetchSourceContext);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Item)) return false;

            Item item = (Item) o;

            if (version != item.version) return false;
            if (fetchSourceContext != null ? !fetchSourceContext.equals(item.fetchSourceContext) : item.fetchSourceContext != null)
                return false;
            if (!Arrays.equals(storedFields, item.storedFields)) return false;
            if (!id.equals(item.id)) return false;
            if (!index.equals(item.index)) return false;
            if (routing != null ? !routing.equals(item.routing) : item.routing != null) return false;
            if (parent != null ? !parent.equals(item.parent) : item.parent != null) return false;
            if (type != null ? !type.equals(item.type) : item.type != null) return false;
            if (versionType != item.versionType) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = index.hashCode();
            result = 31 * result + (type != null ? type.hashCode() : 0);
            result = 31 * result + id.hashCode();
            result = 31 * result + (routing != null ? routing.hashCode() : 0);
            result = 31 * result + (parent != null ? parent.hashCode() : 0);
            result = 31 * result + (storedFields != null ? Arrays.hashCode(storedFields) : 0);
            result = 31 * result + Long.hashCode(version);
            result = 31 * result + versionType.hashCode();
            result = 31 * result + (fetchSourceContext != null ? fetchSourceContext.hashCode() : 0);
            return result;
        }

        public String toString() {
            return Strings.toString(this);
        }

    }

    String preference;
    boolean realtime = true;
    boolean refresh;
    List<Item> items = new ArrayList<>();

    public List<Item> getItems() {
        return this.items;
    }

    public MultiGetRequest add(Item item) {
        items.add(item);
        return this;
    }

    public MultiGetRequest add(String index, @Nullable String type, String id) {
        items.add(new Item(index, type, id));
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (items.isEmpty()) {
            validationException = ValidateActions.addValidationError("no documents to get", validationException);
        } else {
            for (int i = 0; i < items.size(); i++) {
                Item item = items.get(i);
                if (item.index() == null) {
                    validationException = ValidateActions.addValidationError("index is missing for doc " + i, validationException);
                }
                if (item.id() == null) {
                    validationException = ValidateActions.addValidationError("id is missing for doc " + i, validationException);
                }
            }
        }
        return validationException;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * {@code _local} to prefer local shards, {@code _primary} to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public MultiGetRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
    }

    public boolean realtime() {
        return this.realtime;
    }

    @Override
    public MultiGetRequest realtime(boolean realtime) {
        this.realtime = realtime;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    public MultiGetRequest refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public MultiGetRequest add(@Nullable String defaultIndex, @Nullable String defaultType, @Nullable String[] defaultFields,
            @Nullable FetchSourceContext defaultFetchSource, @Nullable String defaultRouting, XContentParser parser,
            boolean allowExplicitIndex) throws IOException {
        Token token;
        String currentFieldName = null;
        if ((token = parser.nextToken()) != Token.START_OBJECT) {
            final String message = String.format(
                    Locale.ROOT,
                    "unexpected token [%s], expected [%s]",
                    token,
                    Token.START_OBJECT);
            throw new ParsingException(parser.getTokenLocation(), message);
        }
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == Token.START_ARRAY) {
                if ("docs".equals(currentFieldName)) {
                    parseDocuments(parser, this.items, defaultIndex, defaultType, defaultFields, defaultFetchSource, defaultRouting, allowExplicitIndex);
                } else if ("ids".equals(currentFieldName)) {
                    parseIds(parser, this.items, defaultIndex, defaultType, defaultFields, defaultFetchSource, defaultRouting);
                } else {
                    final String message = String.format(
                            Locale.ROOT,
                            "unknown key [%s] for a %s, expected [docs] or [ids]",
                            currentFieldName,
                            token);
                    throw new ParsingException(parser.getTokenLocation(), message);
                }
            } else {
                final String message = String.format(
                        Locale.ROOT,
                        "unexpected token [%s], expected [%s] or [%s]",
                        token,
                        Token.FIELD_NAME,
                        Token.START_ARRAY);
                throw new ParsingException(parser.getTokenLocation(), message);
            }
        }
        return this;
    }

    private static void parseDocuments(XContentParser parser, List<Item> items, @Nullable String defaultIndex, @Nullable String defaultType, @Nullable String[] defaultFields, @Nullable FetchSourceContext defaultFetchSource, @Nullable String defaultRouting, boolean allowExplicitIndex) throws IOException {
        String currentFieldName = null;
        Token token;
        while ((token = parser.nextToken()) != Token.END_ARRAY) {
            if (token != Token.START_OBJECT) {
                throw new IllegalArgumentException("docs array element should include an object");
            }
            String index = defaultIndex;
            String type = defaultType;
            String id = null;
            String routing = defaultRouting;
            String parent = null;
            List<String> storedFields = null;
            long version = Versions.MATCH_ANY;
            VersionType versionType = VersionType.INTERNAL;

            FetchSourceContext fetchSourceContext = FetchSourceContext.FETCH_SOURCE;

            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (INDEX.match(currentFieldName, parser.getDeprecationHandler())) {
                        if (!allowExplicitIndex) {
                            throw new IllegalArgumentException("explicit index in multi get is not allowed");
                        }
                        index = parser.text();
                    } else if (TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                        type = parser.text();
                    } else if (ID.match(currentFieldName, parser.getDeprecationHandler())) {
                        id = parser.text();
                    } else if (ROUTING.match(currentFieldName, parser.getDeprecationHandler())) {
                        routing = parser.text();
                    } else if (PARENT.match(currentFieldName, parser.getDeprecationHandler())) {
                        parent = parser.text();
                    } else if (FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                        throw new ParsingException(parser.getTokenLocation(),
                            "Unsupported field [fields] used, expected [stored_fields] instead");
                    } else if (STORED_FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                        storedFields = new ArrayList<>();
                        storedFields.add(parser.text());
                    } else if (VERSION.match(currentFieldName, parser.getDeprecationHandler())) {
                        version = parser.longValue();
                    } else if (VERSION_TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                        versionType = VersionType.fromString(parser.text());
                    } else if (SOURCE.match(currentFieldName, parser.getDeprecationHandler())) {
                        // check lenient to avoid interpreting the value as string but parse strict in order to provoke an error early on.
                        if (parser.isBooleanValueLenient()) {
                            fetchSourceContext = new FetchSourceContext(parser.booleanValue(), fetchSourceContext.includes(),
                                fetchSourceContext.excludes());
                        } else if (token == Token.VALUE_STRING) {
                            fetchSourceContext = new FetchSourceContext(fetchSourceContext.fetchSource(),
                                new String[]{parser.text()}, fetchSourceContext.excludes());
                        } else {
                            throw new ElasticsearchParseException("illegal type for _source: [{}]", token);
                        }
                    } else {
                        throw new ElasticsearchParseException("failed to parse multi get request. unknown field [{}]", currentFieldName);
                    }
                } else if (token == Token.START_ARRAY) {
                    if (FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                        throw new ParsingException(parser.getTokenLocation(),
                            "Unsupported field [fields] used, expected [stored_fields] instead");
                    } else if (STORED_FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                        storedFields = new ArrayList<>();
                        while ((token = parser.nextToken()) != Token.END_ARRAY) {
                            storedFields.add(parser.text());
                        }
                    } else if (SOURCE.match(currentFieldName, parser.getDeprecationHandler())) {
                        ArrayList<String> includes = new ArrayList<>();
                        while ((token = parser.nextToken()) != Token.END_ARRAY) {
                            includes.add(parser.text());
                        }
                        fetchSourceContext = new FetchSourceContext(fetchSourceContext.fetchSource(), includes.toArray(Strings.EMPTY_ARRAY)
                            , fetchSourceContext.excludes());
                    }

                } else if (token == Token.START_OBJECT) {
                    if (SOURCE.match(currentFieldName, parser.getDeprecationHandler())) {
                        List<String> currentList = null, includes = null, excludes = null;

                        while ((token = parser.nextToken()) != Token.END_OBJECT) {
                            if (token == Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                                if ("includes".equals(currentFieldName) || "include".equals(currentFieldName)) {
                                    currentList = includes != null ? includes : (includes = new ArrayList<>(2));
                                } else if ("excludes".equals(currentFieldName) || "exclude".equals(currentFieldName)) {
                                    currentList = excludes != null ? excludes : (excludes = new ArrayList<>(2));
                                } else {
                                    throw new ElasticsearchParseException("source definition may not contain [{}]", parser.text());
                                }
                            } else if (token == Token.START_ARRAY) {
                                while ((token = parser.nextToken()) != Token.END_ARRAY) {
                                    currentList.add(parser.text());
                                }
                            } else if (token.isValue()) {
                                currentList.add(parser.text());
                            } else {
                                throw new ElasticsearchParseException("unexpected token while parsing source settings");
                            }
                        }

                        fetchSourceContext = new FetchSourceContext(fetchSourceContext.fetchSource(),
                                includes == null ? Strings.EMPTY_ARRAY : includes.toArray(new String[includes.size()]),
                                excludes == null ? Strings.EMPTY_ARRAY : excludes.toArray(new String[excludes.size()]));
                    }
                }
            }
            String[] aFields;
            if (storedFields != null) {
                aFields = storedFields.toArray(new String[storedFields.size()]);
            } else {
                aFields = defaultFields;
            }
            items.add(new Item(index, type, id).routing(routing).storedFields(aFields).parent(parent).version(version).versionType(versionType)
                    .fetchSourceContext(fetchSourceContext == FetchSourceContext.FETCH_SOURCE ? defaultFetchSource : fetchSourceContext));
        }
    }

    public static void parseIds(XContentParser parser, List<Item> items, @Nullable String defaultIndex, @Nullable String defaultType, @Nullable String[] defaultFields, @Nullable FetchSourceContext defaultFetchSource, @Nullable String defaultRouting) throws IOException {
        Token token;
        while ((token = parser.nextToken()) != Token.END_ARRAY) {
            if (!token.isValue()) {
                throw new IllegalArgumentException("ids array element should only contain ids");
            }
            items.add(new Item(defaultIndex, defaultType, parser.text()).storedFields(defaultFields).fetchSourceContext(defaultFetchSource).routing(defaultRouting));
        }
    }

    public static void parseIds(XContentParser parser, List<Item> items) throws IOException {
        parseIds(parser, items, null, null, null, null, null);
    }

    @Override
    public Iterator<Item> iterator() {
        return Collections.unmodifiableCollection(items).iterator();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        preference = in.readOptionalString();
        refresh = in.readBoolean();
        realtime = in.readBoolean();

        int size = in.readVInt();
        items = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            items.add(Item.readItem(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(preference);
        out.writeBoolean(refresh);
        out.writeBoolean(realtime);

        out.writeVInt(items.size());
        for (Item item : items) {
            item.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(DOCS.getPreferredName());
        for (Item item : items) {
            builder.value(item);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

}
