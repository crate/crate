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

package org.elasticsearch.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.transport.RemoteClusterAware;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.lucene.Lucene.readExplanation;
import static org.elasticsearch.common.lucene.Lucene.writeExplanation;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureFieldName;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseFieldsValue;
import static org.elasticsearch.search.fetch.subphase.highlight.HighlightField.readHighlightField;

/**
 * A single search hit.
 *
 * @see SearchHits
 */
public final class SearchHit implements Streamable, ToXContentObject, Iterable<DocumentField> {

    private transient int docId;

    private static final float DEFAULT_SCORE = Float.NEGATIVE_INFINITY;
    private float score = DEFAULT_SCORE;

    private Text id;
    private Text type;

    private NestedIdentity nestedIdentity;

    private long version = -1;

    private BytesReference source;

    private Map<String, DocumentField> fields = emptyMap();

    private Map<String, HighlightField> highlightFields = null;

    private SearchSortValues sortValues = SearchSortValues.EMPTY;

    private String[] matchedQueries = Strings.EMPTY_ARRAY;

    private Explanation explanation;

    @Nullable
    private SearchShardTarget shard;

    //These two fields normally get set when setting the shard target, so they hold the same values as the target thus don't get
    //serialized over the wire. When parsing hits back from xcontent though, in most of the cases (whenever explanation is disabled)
    //we can't rebuild the shard target object so we need to set these manually for users retrieval.
    private transient String index;
    private transient String clusterAlias;

    private Map<String, Object> sourceAsMap;

    private Map<String, SearchHits> innerHits;

    private SearchHit() {

    }

    public SearchHit(int docId) {
        this(docId, null, null, null);
    }

    public SearchHit(int docId, String id, Text type, Map<String, DocumentField> fields) {
        this(docId, id, type, null, fields);
    }

    public SearchHit(int nestedTopDocId, String id, Text type, NestedIdentity nestedIdentity, Map<String, DocumentField> fields) {
        this.docId = nestedTopDocId;
        if (id != null) {
            this.id = new Text(id);
        } else {
            this.id = null;
        }
        this.type = type;
        this.nestedIdentity = nestedIdentity;
        this.fields = fields;
    }

    public int docId() {
        return this.docId;
    }

    public void score(float score) {
        this.score = score;
    }

    /**
     * The score.
     */
    public float getScore() {
        return this.score;
    }

    public void version(long version) {
        this.version = version;
    }

    /**
     * The version of the hit.
     */
    public long getVersion() {
        return this.version;
    }

    /**
     * The index of the hit.
     */
    public String getIndex() {
        return this.index;
    }

    /**
     * The id of the document.
     */
    public String getId() {
        return id != null ? id.string() : null;
    }

    /**
     * The type of the document.
     */
    public String getType() {
        return type != null ? type.string() : null;
    }

    /**
     * If this is a nested hit then nested reference information is returned otherwise <code>null</code> is returned.
     */
    public NestedIdentity getNestedIdentity() {
        return nestedIdentity;
    }

    /**
     * Returns bytes reference, also uncompress the source if needed.
     */
    public BytesReference getSourceRef() {
        if (this.source == null) {
            return null;
        }

        try {
            this.source = CompressorFactory.uncompressIfNeeded(this.source);
            return this.source;
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to decompress source", e);
        }
    }

    /**
     * Sets representation, might be compressed....
     */
    public SearchHit sourceRef(BytesReference source) {
        this.source = source;
        this.sourceAsMap = null;
        return this;
    }

    /**
     * Is the source available or not. A source with no fields will return true. This will return false if {@code fields} doesn't contain
     * {@code _source} or if source is disabled in the mapping.
     */
    public boolean hasSource() {
        return source != null;
    }

    /**
     * The source of the document as string (can be {@code null}).
     */
    public String getSourceAsString() {
        if (source == null) {
            return null;
        }
        try {
            return XContentHelper.convertToJson(getSourceRef(), false);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to convert source to a json string");
        }
    }


    /**
     * The source of the document as a map (can be {@code null}).
     */
    public Map<String, Object> getSourceAsMap() {
        if (source == null) {
            return null;
        }
        if (sourceAsMap != null) {
            return sourceAsMap;
        }

        sourceAsMap = SourceLookup.sourceAsMap(source);
        return sourceAsMap;
    }

    @Override
    public Iterator<DocumentField> iterator() {
        return fields.values().iterator();
    }

    /**
     * The hit field matching the given field name.
     */
    public DocumentField field(String fieldName) {
        return getFields().get(fieldName);
    }

    /**
     * A map of hit fields (from field name to hit fields) if additional fields
     * were required to be loaded.
     */
    public Map<String, DocumentField> getFields() {
        return fields == null ? emptyMap() : fields;
    }

    // returns the fields without handling null cases
    public Map<String, DocumentField> fieldsOrNull() {
        return fields;
    }

    public void fields(Map<String, DocumentField> fields) {
        this.fields = fields;
    }

    /**
     * A map of highlighted fields.
     */
    public Map<String, HighlightField> getHighlightFields() {
        return highlightFields == null ? emptyMap() : highlightFields;
    }

    public void highlightFields(Map<String, HighlightField> highlightFields) {
        this.highlightFields = highlightFields;
    }

    public void sortValues(Object[] sortValues, DocValueFormat[] sortValueFormats) {
        sortValues(new SearchSortValues(sortValues, sortValueFormats));
    }

    public void sortValues(SearchSortValues sortValues) {
        this.sortValues = sortValues;
    }

    /**
     * An array of the sort values used.
     */
    public Object[] getSortValues() {
        return sortValues.sortValues();
    }

    /**
     * If enabled, the explanation of the search hit.
     */
    public Explanation getExplanation() {
        return explanation;
    }

    public void explanation(Explanation explanation) {
        this.explanation = explanation;
    }

    /**
     * The shard of the search hit.
     */
    public SearchShardTarget getShard() {
        return shard;
    }

    public void shard(SearchShardTarget target) {
        if (innerHits != null) {
            for (SearchHits innerHits : innerHits.values()) {
                for (SearchHit innerHit : innerHits) {
                    innerHit.shard(target);
                }
            }
        }

        this.shard = target;
        if (target != null) {
            this.index = target.getIndex();
            this.clusterAlias = target.getClusterAlias();
        }
    }

    /**
     * Returns the cluster alias this hit comes from or null if it comes from a local cluster
     */
    public String getClusterAlias() {
        return clusterAlias;
    }

    public void matchedQueries(String[] matchedQueries) {
        this.matchedQueries = matchedQueries;
    }

    /**
     * The set of query and filter names the query matched with. Mainly makes sense for compound filters and queries.
     */
    public String[] getMatchedQueries() {
        return this.matchedQueries;
    }

    /**
     * @return Inner hits or <code>null</code> if there are none
     */
    public Map<String, SearchHits> getInnerHits() {
        return innerHits;
    }

    public void setInnerHits(Map<String, SearchHits> innerHits) {
        this.innerHits = innerHits;
    }

    public static class Fields {
        static final String _INDEX = "_index";
        static final String _TYPE = "_type";
        static final String _ID = "_id";
        static final String _VERSION = "_version";
        static final String _SCORE = "_score";
        static final String FIELDS = "fields";
        static final String HIGHLIGHT = "highlight";
        static final String SORT = "sort";
        static final String MATCHED_QUERIES = "matched_queries";
        static final String _EXPLANATION = "_explanation";
        static final String VALUE = "value";
        static final String DESCRIPTION = "description";
        static final String DETAILS = "details";
        static final String INNER_HITS = "inner_hits";
        static final String _SHARD = "_shard";
        static final String _NODE = "_node";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toInnerXContent(builder, params);
        builder.endObject();
        return builder;
    }

    // public because we render hit as part of completion suggestion option
    public XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException {
        List<DocumentField> metaFields = new ArrayList<>();
        List<DocumentField> otherFields = new ArrayList<>();
        if (fields != null && !fields.isEmpty()) {
            for (DocumentField field : fields.values()) {
                if (field.getValues().isEmpty()) {
                    continue;
                }
                if (field.isMetadataField()) {
                    metaFields.add(field);
                } else {
                    otherFields.add(field);
                }
            }
        }

        // For inner_hit hits shard is null and that is ok, because the parent search hit has all this information.
        // Even if this was included in the inner_hit hits this would be the same, so better leave it out.
        if (getExplanation() != null && shard != null) {
            builder.field(Fields._SHARD, shard.getShardId());
            builder.field(Fields._NODE, shard.getNodeIdText());
        }
        if (index != null) {
            builder.field(Fields._INDEX, RemoteClusterAware.buildRemoteIndexName(clusterAlias, index));
        }
        if (type != null) {
            builder.field(Fields._TYPE, type);
        }
        if (id != null) {
            builder.field(Fields._ID, id);
        }
        if (nestedIdentity != null) {
            nestedIdentity.toXContent(builder, params);
        }
        if (version != -1) {
            builder.field(Fields._VERSION, version);
        }
        if (Float.isNaN(score)) {
            builder.nullField(Fields._SCORE);
        } else {
            builder.field(Fields._SCORE, score);
        }
        for (DocumentField field : metaFields) {
            // _ignored is the only multi-valued meta field
            // TODO: can we avoid having an exception here?
            if (field.getName().equals(IgnoredFieldMapper.NAME)) {
                builder.field(field.getName(), field.getValues());
            } else {
                builder.field(field.getName(), field.<Object>getValue());
            }
        }
        if (source != null) {
            XContentHelper.writeRawField(SourceFieldMapper.NAME, source, builder, params);
        }
        if (!otherFields.isEmpty()) {
            builder.startObject(Fields.FIELDS);
            for (DocumentField field : otherFields) {
                field.toXContent(builder, params);
            }
            builder.endObject();
        }
        if (highlightFields != null && !highlightFields.isEmpty()) {
            builder.startObject(Fields.HIGHLIGHT);
            for (HighlightField field : highlightFields.values()) {
                field.toXContent(builder, params);
            }
            builder.endObject();
        }
        sortValues.toXContent(builder, params);
        if (matchedQueries.length > 0) {
            builder.startArray(Fields.MATCHED_QUERIES);
            for (String matchedFilter : matchedQueries) {
                builder.value(matchedFilter);
            }
            builder.endArray();
        }
        if (getExplanation() != null) {
            builder.field(Fields._EXPLANATION);
            buildExplanation(builder, getExplanation());
        }
        if (innerHits != null) {
            builder.startObject(Fields.INNER_HITS);
            for (Map.Entry<String, SearchHits> entry : innerHits.entrySet()) {
                builder.startObject(entry.getKey());
                entry.getValue().toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        return builder;
    }

    /**
     * This parser outputs a temporary map of the objects needed to create the
     * SearchHit instead of directly creating the SearchHit. The reason for this
     * is that this way we can reuse the parser when parsing xContent from
     * {@link org.elasticsearch.search.suggest.completion.CompletionSuggestion.Entry.Option} which unfortunately inlines
     * the output of
     * {@link #toInnerXContent(XContentBuilder, org.elasticsearch.common.xcontent.ToXContent.Params)}
     * of the included search hit. The output of the map is used to create the
     * actual SearchHit instance via {@link #createFromMap(Map)}
     */
    private static ObjectParser<Map<String, Object>, Void> MAP_PARSER = new ObjectParser<>("innerHitParser", true, HashMap::new);

    static {
        declareInnerHitsParseFields(MAP_PARSER);
    }

    public static SearchHit fromXContent(XContentParser parser) {
        return createFromMap(MAP_PARSER.apply(parser, null));
    }

    public static void declareInnerHitsParseFields(ObjectParser<Map<String, Object>, Void> parser) {
        declareMetaDataFields(parser);
        parser.declareString((map, value) -> map.put(Fields._TYPE, new Text(value)), new ParseField(Fields._TYPE));
        parser.declareString((map, value) -> map.put(Fields._INDEX, value), new ParseField(Fields._INDEX));
        parser.declareString((map, value) -> map.put(Fields._ID, value), new ParseField(Fields._ID));
        parser.declareString((map, value) -> map.put(Fields._NODE, value), new ParseField(Fields._NODE));
        parser.declareField((map, value) -> map.put(Fields._SCORE, value), SearchHit::parseScore, new ParseField(Fields._SCORE),
                ValueType.FLOAT_OR_NULL);
        parser.declareLong((map, value) -> map.put(Fields._VERSION, value), new ParseField(Fields._VERSION));
        parser.declareField((map, value) -> map.put(Fields._SHARD, value), (p, c) -> ShardId.fromString(p.text()),
                new ParseField(Fields._SHARD), ValueType.STRING);
        parser.declareObject((map, value) -> map.put(SourceFieldMapper.NAME, value), (p, c) -> parseSourceBytes(p),
                new ParseField(SourceFieldMapper.NAME));
        parser.declareObject((map, value) -> map.put(Fields.HIGHLIGHT, value), (p, c) -> parseHighlightFields(p),
                new ParseField(Fields.HIGHLIGHT));
        parser.declareObject((map, value) -> {
            Map<String, DocumentField> fieldMap = get(Fields.FIELDS, map, new HashMap<String, DocumentField>());
            fieldMap.putAll(value);
            map.put(Fields.FIELDS, fieldMap);
        }, (p, c) -> parseFields(p), new ParseField(Fields.FIELDS));
        parser.declareObject((map, value) -> map.put(Fields._EXPLANATION, value), (p, c) -> parseExplanation(p),
                new ParseField(Fields._EXPLANATION));
        parser.declareObject((map, value) -> map.put(NestedIdentity._NESTED, value), NestedIdentity::fromXContent,
                new ParseField(NestedIdentity._NESTED));
        parser.declareObject((map, value) -> map.put(Fields.INNER_HITS, value), (p,c) -> parseInnerHits(p),
                new ParseField(Fields.INNER_HITS));
        parser.declareStringArray((map, list) -> map.put(Fields.MATCHED_QUERIES, list), new ParseField(Fields.MATCHED_QUERIES));
        parser.declareField((map, list) -> map.put(Fields.SORT, list), SearchSortValues::fromXContent, new ParseField(Fields.SORT),
                ValueType.OBJECT_ARRAY);
    }

    public static SearchHit createFromMap(Map<String, Object> values) {
        String id = get(Fields._ID, values, null);
        Text type = get(Fields._TYPE, values, null);
        NestedIdentity nestedIdentity = get(NestedIdentity._NESTED, values, null);
        Map<String, DocumentField> fields = get(Fields.FIELDS, values, Collections.emptyMap());

        SearchHit searchHit = new SearchHit(-1, id, type, nestedIdentity, fields);
        String index = get(Fields._INDEX, values, null);
        String clusterAlias = null;
        if (index != null) {
            int indexOf = index.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR);
            if (indexOf > 0) {
                clusterAlias = index.substring(0, indexOf);
                index = index.substring(indexOf + 1);
            }
        }
        ShardId shardId = get(Fields._SHARD, values, null);
        String nodeId = get(Fields._NODE, values, null);
        if (shardId != null && nodeId != null) {
            assert shardId.getIndexName().equals(index);
            searchHit.shard(new SearchShardTarget(nodeId, shardId, clusterAlias, OriginalIndices.NONE));
        } else {
            //these fields get set anyways when setting the shard target,
            //but we set them explicitly when we don't have enough info to rebuild the shard target
            searchHit.index = index;
            searchHit.clusterAlias = clusterAlias;
        }
        searchHit.score(get(Fields._SCORE, values, DEFAULT_SCORE));
        searchHit.version(get(Fields._VERSION, values, -1L));
        searchHit.sortValues(get(Fields.SORT, values, SearchSortValues.EMPTY));
        searchHit.highlightFields(get(Fields.HIGHLIGHT, values, null));
        searchHit.sourceRef(get(SourceFieldMapper.NAME, values, null));
        searchHit.explanation(get(Fields._EXPLANATION, values, null));
        searchHit.setInnerHits(get(Fields.INNER_HITS, values, null));
        List<String> matchedQueries = get(Fields.MATCHED_QUERIES, values, null);
        if (matchedQueries != null) {
            searchHit.matchedQueries(matchedQueries.toArray(new String[0]));
        }
        return searchHit;
    }

    @SuppressWarnings("unchecked")
    private static <T> T get(String key, Map<String, Object> map, T defaultValue) {
        return (T) map.getOrDefault(key, defaultValue);
    }

    private static float parseScore(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER || parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return parser.floatValue();
        } else {
            return Float.NaN;
        }
    }

    private static BytesReference parseSourceBytes(XContentParser parser) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent())) {
            // the original document gets slightly modified: whitespaces or
            // pretty printing are not preserved,
            // it all depends on the current builder settings
            builder.copyCurrentStructure(parser);
            return BytesReference.bytes(builder);
        }
    }

    /**
     * we need to declare parse fields for each metadata field, except for _ID, _INDEX and _TYPE which are
     * handled individually. All other fields are parsed to an entry in the fields map
     */
    private static void declareMetaDataFields(ObjectParser<Map<String, Object>, Void> parser) {
        for (String metadatafield : MapperService.getAllMetaFields()) {
            if (metadatafield.equals(Fields._ID) == false && metadatafield.equals(Fields._INDEX) == false
                    && metadatafield.equals(Fields._TYPE) == false) {
                if (metadatafield.equals(IgnoredFieldMapper.NAME)) {
                    parser.declareObjectArray((map, list) -> {
                            @SuppressWarnings("unchecked")
                            Map<String, DocumentField> fieldMap = (Map<String, DocumentField>) map.computeIfAbsent(Fields.FIELDS,
                                v -> new HashMap<String, DocumentField>());
                            DocumentField field = new DocumentField(metadatafield, list);
                            fieldMap.put(field.getName(), field);
                        }, (p, c) -> parseFieldsValue(p),
                        new ParseField(metadatafield));
                } else {
                    parser.declareField((map, field) -> {
                            @SuppressWarnings("unchecked")
                            Map<String, DocumentField> fieldMap = (Map<String, DocumentField>) map.computeIfAbsent(Fields.FIELDS,
                                v -> new HashMap<String, DocumentField>());
                            fieldMap.put(field.getName(), field);
                        }, (p, c) -> new DocumentField(metadatafield, Collections.singletonList(parseFieldsValue(p))),
                        new ParseField(metadatafield), ValueType.VALUE);
                }
            }
        }
    }

    private static Map<String, DocumentField> parseFields(XContentParser parser) throws IOException {
        Map<String, DocumentField> fields = new HashMap<>();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            DocumentField field = DocumentField.fromXContent(parser);
            fields.put(field.getName(), field);
        }
        return fields;
    }

    private static Map<String, SearchHits> parseInnerHits(XContentParser parser) throws IOException {
        Map<String, SearchHits> innerHits = new HashMap<>();
        while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
            String name = parser.currentName();
            ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            ensureFieldName(parser, parser.nextToken(), SearchHits.Fields.HITS);
            innerHits.put(name, SearchHits.fromXContent(parser));
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
        }
        return innerHits;
    }

    private static Map<String, HighlightField> parseHighlightFields(XContentParser parser) throws IOException {
        Map<String, HighlightField> highlightFields = new HashMap<>();
        while((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            HighlightField highlightField = HighlightField.fromXContent(parser);
            highlightFields.put(highlightField.getName(), highlightField);
        }
        return highlightFields;
    }

    private static Explanation parseExplanation(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        XContentParser.Token token;
        Float value = null;
        String description = null;
        List<Explanation> details = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
            String currentFieldName = parser.currentName();
            token = parser.nextToken();
            if (Fields.VALUE.equals(currentFieldName)) {
                value = parser.floatValue();
            } else if (Fields.DESCRIPTION.equals(currentFieldName)) {
                description = parser.textOrNull();
            } else if (Fields.DETAILS.equals(currentFieldName)) {
                ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser::getTokenLocation);
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    details.add(parseExplanation(parser));
                }
            } else {
                parser.skipChildren();
            }
        }
        if (value == null) {
            throw new ParsingException(parser.getTokenLocation(), "missing explanation value");
        }
        if (description == null) {
            throw new ParsingException(parser.getTokenLocation(), "missing explanation description");
        }
        return Explanation.match(value, description, details);
    }

    private void buildExplanation(XContentBuilder builder, Explanation explanation) throws IOException {
        builder.startObject();
        builder.field(Fields.VALUE, explanation.getValue());
        builder.field(Fields.DESCRIPTION, explanation.getDescription());
        Explanation[] innerExps = explanation.getDetails();
        if (innerExps != null) {
            builder.startArray(Fields.DETAILS);
            for (Explanation exp : innerExps) {
                buildExplanation(builder, exp);
            }
            builder.endArray();
        }
        builder.endObject();
    }

    public static SearchHit readSearchHit(StreamInput in) throws IOException {
        SearchHit hit = new SearchHit();
        hit.readFrom(in);
        return hit;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        score = in.readFloat();
        id = in.readOptionalText();
        type = in.readOptionalText();
        nestedIdentity = in.readOptionalWriteable(NestedIdentity::new);
        version = in.readLong();
        source = in.readBytesReference();
        if (source.length() == 0) {
            source = null;
        }
        if (in.readBoolean()) {
            explanation = readExplanation(in);
        }
        int size = in.readVInt();
        if (size == 0) {
            fields = emptyMap();
        } else if (size == 1) {
            DocumentField hitField = DocumentField.readDocumentField(in);
            fields = singletonMap(hitField.getName(), hitField);
        } else {
            Map<String, DocumentField> fields = new HashMap<>();
            for (int i = 0; i < size; i++) {
                DocumentField hitField = DocumentField.readDocumentField(in);
                fields.put(hitField.getName(), hitField);
            }
            this.fields = unmodifiableMap(fields);
        }

        size = in.readVInt();
        if (size == 0) {
            highlightFields = emptyMap();
        } else if (size == 1) {
            HighlightField field = readHighlightField(in);
            highlightFields = singletonMap(field.name(), field);
        } else {
            Map<String, HighlightField> highlightFields = new HashMap<>();
            for (int i = 0; i < size; i++) {
                HighlightField field = readHighlightField(in);
                highlightFields.put(field.name(), field);
            }
            this.highlightFields = unmodifiableMap(highlightFields);
        }

        sortValues = new SearchSortValues(in);

        size = in.readVInt();
        if (size > 0) {
            matchedQueries = new String[size];
            for (int i = 0; i < size; i++) {
                matchedQueries[i] = in.readString();
            }
        }
        // we call the setter here because that also sets the local index parameter
        shard(in.readOptionalWriteable(SearchShardTarget::new));
        size = in.readVInt();
        if (size > 0) {
            innerHits = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                String key = in.readString();
                SearchHits value = SearchHits.readSearchHits(in);
                innerHits.put(key, value);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeFloat(score);
        out.writeOptionalText(id);
        out.writeOptionalText(type);
        out.writeOptionalWriteable(nestedIdentity);
        out.writeLong(version);
        out.writeBytesReference(source);
        if (explanation == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            writeExplanation(out, explanation);
        }
        if (fields == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(fields.size());
            for (DocumentField hitField : getFields().values()) {
                hitField.writeTo(out);
            }
        }
        if (highlightFields == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(highlightFields.size());
            for (HighlightField highlightField : highlightFields.values()) {
                highlightField.writeTo(out);
            }
        }
        sortValues.writeTo(out);

        if (matchedQueries.length == 0) {
            out.writeVInt(0);
        } else {
            out.writeVInt(matchedQueries.length);
            for (String matchedFilter : matchedQueries) {
                out.writeString(matchedFilter);
            }
        }
        out.writeOptionalWriteable(shard);
        if (innerHits == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(innerHits.size());
            for (Map.Entry<String, SearchHits> entry : innerHits.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SearchHit other = (SearchHit) obj;
        return Objects.equals(id, other.id)
                && Objects.equals(type, other.type)
                && Objects.equals(nestedIdentity, other.nestedIdentity)
                && Objects.equals(version, other.version)
                && Objects.equals(source, other.source)
                && Objects.equals(fields, other.fields)
                && Objects.equals(getHighlightFields(), other.getHighlightFields())
                && Arrays.equals(matchedQueries, other.matchedQueries)
                && Objects.equals(explanation, other.explanation)
                && Objects.equals(shard, other.shard)
                && Objects.equals(innerHits, other.innerHits)
                && Objects.equals(index, other.index)
                && Objects.equals(clusterAlias, other.clusterAlias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, nestedIdentity, version, source, fields, getHighlightFields(), Arrays.hashCode(matchedQueries),
                explanation, shard, innerHits, index, clusterAlias);
    }

    /**
     * Encapsulates the nested identity of a hit.
     */
    public static final class NestedIdentity implements Writeable, ToXContentFragment {

        private static final String _NESTED = "_nested";
        private static final String FIELD = "field";
        private static final String OFFSET = "offset";

        private final Text field;
        private final int offset;
        private final NestedIdentity child;

        public NestedIdentity(String field, int offset, NestedIdentity child) {
            this.field = new Text(field);
            this.offset = offset;
            this.child = child;
        }

        NestedIdentity(StreamInput in) throws IOException {
            field = in.readOptionalText();
            offset = in.readInt();
            child = in.readOptionalWriteable(NestedIdentity::new);
        }

        /**
         * Returns the nested field in the source this hit originates from
         */
        public Text getField() {
            return field;
        }

        /**
         * Returns the offset in the nested array of objects in the source this hit
         */
        public int getOffset() {
            return offset;
        }

        /**
         * Returns the next child nested level if there is any, otherwise <code>null</code> is returned.
         *
         * In the case of mappings with multiple levels of nested object fields
         */
        public NestedIdentity getChild() {
            return child;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalText(field);
            out.writeInt(offset);
            out.writeOptionalWriteable(child);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(_NESTED);
            return innerToXContent(builder, params);
        }

        /**
         * Rendering of the inner XContent object without the leading field name. This way the structure innerToXContent renders and
         * fromXContent parses correspond to each other.
         */
        XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (field != null) {
                builder.field(FIELD, field);
            }
            if (offset != -1) {
                builder.field(OFFSET, offset);
            }
            if (child != null) {
                builder = child.toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<NestedIdentity, Void> PARSER = new ConstructingObjectParser<>("nested_identity", true,
                ctorArgs -> new NestedIdentity((String) ctorArgs[0], (int) ctorArgs[1], (NestedIdentity) ctorArgs[2]));
        static {
            PARSER.declareString(constructorArg(), new ParseField(FIELD));
            PARSER.declareInt(constructorArg(), new ParseField(OFFSET));
            PARSER.declareObject(optionalConstructorArg(), PARSER, new ParseField(_NESTED));
        }

        static NestedIdentity fromXContent(XContentParser parser, Void context) {
            return fromXContent(parser);
        }

        public static NestedIdentity fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            NestedIdentity other = (NestedIdentity) obj;
            return Objects.equals(field, other.field) &&
                    Objects.equals(offset, other.offset) &&
                    Objects.equals(child, other.child);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, offset, child);
        }
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
