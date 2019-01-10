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
package org.elasticsearch.index.query;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder.ScriptField;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentParser.Token.END_OBJECT;

public final class InnerHitBuilder implements Writeable, ToXContentObject {

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField IGNORE_UNMAPPED = new ParseField("ignore_unmapped");
    public static final QueryBuilder DEFAULT_INNER_HIT_QUERY = new MatchAllQueryBuilder();
    public static final ParseField COLLAPSE_FIELD = new ParseField("collapse");
    public static final ParseField FIELD_FIELD = new ParseField("field");

    private static final ObjectParser<InnerHitBuilder, Void> PARSER = new ObjectParser<>("inner_hits", InnerHitBuilder::new);

    static {
        PARSER.declareString(InnerHitBuilder::setName, NAME_FIELD);
        PARSER.declareBoolean((innerHitBuilder, value) -> innerHitBuilder.ignoreUnmapped = value, IGNORE_UNMAPPED);
        PARSER.declareInt(InnerHitBuilder::setFrom, SearchSourceBuilder.FROM_FIELD);
        PARSER.declareInt(InnerHitBuilder::setSize, SearchSourceBuilder.SIZE_FIELD);
        PARSER.declareBoolean(InnerHitBuilder::setExplain, SearchSourceBuilder.EXPLAIN_FIELD);
        PARSER.declareBoolean(InnerHitBuilder::setVersion, SearchSourceBuilder.VERSION_FIELD);
        PARSER.declareBoolean(InnerHitBuilder::setTrackScores, SearchSourceBuilder.TRACK_SCORES_FIELD);
        PARSER.declareStringArray(InnerHitBuilder::setStoredFieldNames, SearchSourceBuilder.STORED_FIELDS_FIELD);
        PARSER.declareObjectArray(InnerHitBuilder::setDocValueFields,
                (p,c) -> FieldAndFormat.fromXContent(p), SearchSourceBuilder.DOCVALUE_FIELDS_FIELD);
        PARSER.declareField((p, i, c) -> {
            try {
                Set<ScriptField> scriptFields = new HashSet<>();
                for (XContentParser.Token token = p.nextToken(); token != END_OBJECT; token = p.nextToken()) {
                    scriptFields.add(new ScriptField(p));
                }
                i.setScriptFields(scriptFields);
            } catch (IOException e) {
                throw new ParsingException(p.getTokenLocation(), "Could not parse inner script definition", e);
            }
        }, SearchSourceBuilder.SCRIPT_FIELDS_FIELD, ObjectParser.ValueType.OBJECT);
        PARSER.declareField((p, i, c) -> i.setSorts(SortBuilder.fromXContent(p)), SearchSourceBuilder.SORT_FIELD,
                ObjectParser.ValueType.OBJECT_ARRAY);
        PARSER.declareField((p, i, c) -> {
            try {
                i.setFetchSourceContext(FetchSourceContext.fromXContent(p));
            } catch (IOException e) {
                throw new ParsingException(p.getTokenLocation(), "Could not parse inner _source definition", e);
            }
        }, SearchSourceBuilder._SOURCE_FIELD, ObjectParser.ValueType.OBJECT_ARRAY_BOOLEAN_OR_STRING);
        PARSER.declareObject(InnerHitBuilder::setHighlightBuilder, (p, c) -> HighlightBuilder.fromXContent(p),
                SearchSourceBuilder.HIGHLIGHT_FIELD);
        PARSER.declareField((parser, builder, context) -> {
            Boolean isParsedCorrectly = false;
            String field;
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                if (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                    if (FIELD_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                        if (parser.nextToken() == XContentParser.Token.VALUE_STRING){
                            field = parser.text();
                            if (parser.nextToken() == XContentParser.Token.END_OBJECT){
                                isParsedCorrectly = true;
                                CollapseBuilder cb = new CollapseBuilder(field);
                                builder.setInnerCollapse(cb);
                            }
                        }
                    }
                }
            }
            if (isParsedCorrectly == false) {
                throw new ParsingException(parser.getTokenLocation(), "Invalid token in the inner collapse");
            }

        }, COLLAPSE_FIELD, ObjectParser.ValueType.OBJECT);
    }

    private String name;
    private boolean ignoreUnmapped;

    private int from;
    private int size = 3;
    private boolean explain;
    private boolean version;
    private boolean trackScores;

    private StoredFieldsContext storedFieldsContext;
    private QueryBuilder query = DEFAULT_INNER_HIT_QUERY;
    private List<SortBuilder<?>> sorts;
    private List<FieldAndFormat> docValueFields;
    private Set<ScriptField> scriptFields;
    private HighlightBuilder highlightBuilder;
    private FetchSourceContext fetchSourceContext;
    private CollapseBuilder innerCollapseBuilder = null;

    public InnerHitBuilder() {
        this.name = null;
    }

    public InnerHitBuilder(String name) {
        this.name = name;
    }


    /**
     * Read from a stream.
     */
    public InnerHitBuilder(StreamInput in) throws IOException {
        name = in.readOptionalString();
        if (in.getVersion().before(Version.V_5_5_0)) {
            in.readOptionalString();
            in.readOptionalString();
        }
        if (in.getVersion().onOrAfter(Version.V_5_2_0)) {
            ignoreUnmapped = in.readBoolean();
        }
        from = in.readVInt();
        size = in.readVInt();
        explain = in.readBoolean();
        version = in.readBoolean();
        trackScores = in.readBoolean();
        storedFieldsContext = in.readOptionalWriteable(StoredFieldsContext::new);
        if (in.getVersion().before(Version.V_6_4_0)) {
            List<String> fieldList = (List<String>) in.readGenericValue();
            if (fieldList == null) {
                docValueFields = null;
            } else {
                docValueFields = fieldList.stream()
                        .map(field -> new FieldAndFormat(field, null))
                        .collect(Collectors.toList());
            }
        } else {
            docValueFields = in.readBoolean() ? in.readList(FieldAndFormat::new) : null;
        }
        if (in.readBoolean()) {
            int size = in.readVInt();
            scriptFields = new HashSet<>(size);
            for (int i = 0; i < size; i++) {
                scriptFields.add(new ScriptField(in));
            }
        }
        fetchSourceContext = in.readOptionalWriteable(FetchSourceContext::new);
        if (in.readBoolean()) {
            int size = in.readVInt();
            sorts = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                sorts.add(in.readNamedWriteable(SortBuilder.class));
            }
        }
        highlightBuilder = in.readOptionalWriteable(HighlightBuilder::new);
        if (in.getVersion().before(Version.V_5_5_0)) {
            /**
             * this is needed for BWC with nodes pre 5.5
             */
            in.readNamedWriteable(QueryBuilder.class);
            boolean hasChildren = in.readBoolean();
            assert hasChildren == false;
        }
        if (in.getVersion().onOrAfter(Version.V_6_4_0)) {
            this.innerCollapseBuilder = in.readOptionalWriteable(CollapseBuilder::new);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_5_5_0)) {
            throw new IOException("Invalid output version, must >= " + Version.V_5_5_0.toString());
        }
        out.writeOptionalString(name);
        out.writeBoolean(ignoreUnmapped);
        out.writeVInt(from);
        out.writeVInt(size);
        out.writeBoolean(explain);
        out.writeBoolean(version);
        out.writeBoolean(trackScores);
        out.writeOptionalWriteable(storedFieldsContext);
        if (out.getVersion().before(Version.V_6_4_0)) {
            out.writeGenericValue(docValueFields == null
                    ? null
                    : docValueFields.stream().map(ff -> ff.field).collect(Collectors.toList()));
        } else {
            out.writeBoolean(docValueFields != null);
            if (docValueFields != null) {
                out.writeList(docValueFields);
            }
        }
        boolean hasScriptFields = scriptFields != null;
        out.writeBoolean(hasScriptFields);
        if (hasScriptFields) {
            out.writeVInt(scriptFields.size());
            Iterator<ScriptField> iterator = scriptFields.stream()
                    .sorted(Comparator.comparing(ScriptField::fieldName)).iterator();
            while (iterator.hasNext()) {
                iterator.next().writeTo(out);
            }
        }
        out.writeOptionalWriteable(fetchSourceContext);
        boolean hasSorts = sorts != null;
        out.writeBoolean(hasSorts);
        if (hasSorts) {
            out.writeVInt(sorts.size());
            for (SortBuilder<?> sort : sorts) {
                out.writeNamedWriteable(sort);
            }
        }
        out.writeOptionalWriteable(highlightBuilder);
        if (out.getVersion().onOrAfter(Version.V_6_4_0)) {
            out.writeOptionalWriteable(innerCollapseBuilder);
        }
    }

    /**
     * BWC serialization for nested {@link InnerHitBuilder}.
     * Should only be used to send nested inner hits to nodes pre 5.5.
     */
    protected void writeToNestedBWC(StreamOutput out, QueryBuilder query, String nestedPath) throws IOException {
        assert out.getVersion().before(Version.V_5_5_0) :
            "invalid output version, must be < " + Version.V_5_5_0.toString();
        writeToBWC(out, query, nestedPath, null);
    }

    /**
     * BWC serialization for collapsing {@link InnerHitBuilder}.
     * Should only be used to send collapsing inner hits to nodes pre 5.5.
     */
    public void writeToCollapseBWC(StreamOutput out) throws IOException {
        assert out.getVersion().before(Version.V_5_5_0) :
            "invalid output version, must be < " + Version.V_5_5_0.toString();
        writeToBWC(out, new MatchAllQueryBuilder(), null, null);
    }

    /**
     * BWC serialization for parent/child {@link InnerHitBuilder}.
     * Should only be used to send hasParent or hasChild inner hits to nodes pre 5.5.
     */
    public void writeToParentChildBWC(StreamOutput out, QueryBuilder query, String parentChildPath) throws IOException {
        assert(out.getVersion().before(Version.V_5_5_0)) :
            "invalid output version, must be < " + Version.V_5_5_0.toString();
        writeToBWC(out, query, null, parentChildPath);
    }

    private void writeToBWC(StreamOutput out,
                            QueryBuilder query,
                            String nestedPath,
                            String parentChildPath) throws IOException {
        out.writeOptionalString(name);
        if (nestedPath != null) {
            out.writeOptionalString(nestedPath);
            out.writeOptionalString(null);
        } else {
            out.writeOptionalString(null);
            out.writeOptionalString(parentChildPath);
        }
        if (out.getVersion().onOrAfter(Version.V_5_2_0)) {
            out.writeBoolean(ignoreUnmapped);
        }
        out.writeVInt(from);
        out.writeVInt(size);
        out.writeBoolean(explain);
        out.writeBoolean(version);
        out.writeBoolean(trackScores);
        out.writeOptionalWriteable(storedFieldsContext);
        out.writeGenericValue(docValueFields == null
                ? null
                : docValueFields.stream().map(ff -> ff.field).collect(Collectors.toList()));
        boolean hasScriptFields = scriptFields != null;
        out.writeBoolean(hasScriptFields);
        if (hasScriptFields) {
            out.writeVInt(scriptFields.size());
            Iterator<ScriptField> iterator = scriptFields.stream()
                .sorted(Comparator.comparing(ScriptField::fieldName)).iterator();
            while (iterator.hasNext()) {
                iterator.next().writeTo(out);
            }
        }
        out.writeOptionalWriteable(fetchSourceContext);
        boolean hasSorts = sorts != null;
        out.writeBoolean(hasSorts);
        if (hasSorts) {
            out.writeVInt(sorts.size());
            for (SortBuilder<?> sort : sorts) {
                out.writeNamedWriteable(sort);
            }
        }
        out.writeOptionalWriteable(highlightBuilder);
        out.writeNamedWriteable(query);
        out.writeBoolean(false);
    }

    public String getName() {
        return name;
    }

    public InnerHitBuilder setName(String name) {
        this.name = Objects.requireNonNull(name);
        return this;
    }

    public InnerHitBuilder setIgnoreUnmapped(boolean value) {
        this.ignoreUnmapped = value;
        return this;
    }

    /**
     * Whether to include inner hits in the search response hits if required mappings is missing
     */
    public boolean isIgnoreUnmapped() {
        return ignoreUnmapped;
    }

    public int getFrom() {
        return from;
    }

    public InnerHitBuilder setFrom(int from) {
        if (from < 0) {
            throw new IllegalArgumentException("illegal from value, at least 0 or higher");
        }
        this.from = from;
        return this;
    }

    public int getSize() {
        return size;
    }

    public InnerHitBuilder setSize(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("illegal size value, at least 0 or higher");
        }
        this.size = size;
        return this;
    }

    public boolean isExplain() {
        return explain;
    }

    public InnerHitBuilder setExplain(boolean explain) {
        this.explain = explain;
        return this;
    }

    public boolean isVersion() {
        return version;
    }

    public InnerHitBuilder setVersion(boolean version) {
        this.version = version;
        return this;
    }

    public boolean isTrackScores() {
        return trackScores;
    }

    public InnerHitBuilder setTrackScores(boolean trackScores) {
        this.trackScores = trackScores;
        return this;
    }

    /**
     * Gets the stored fields to load and return.
     *
     * @deprecated Use {@link InnerHitBuilder#getStoredFieldsContext()} instead.
     */
    @Deprecated
    public List<String> getFieldNames() {
        return storedFieldsContext == null ? null : storedFieldsContext.fieldNames();
    }

    /**
     * Sets the stored fields to load and return.
     * If none are specified, the source of the document will be returned.
     *
     * @deprecated Use {@link InnerHitBuilder#setStoredFieldNames(List)} instead.
     */
    @Deprecated
    public InnerHitBuilder setFieldNames(List<String> fieldNames) {
        return setStoredFieldNames(fieldNames);
    }


    /**
     * Gets the stored fields context.
     */
    public StoredFieldsContext getStoredFieldsContext() {
        return storedFieldsContext;
    }

    /**
     * Sets the stored fields to load and return.
     * If none are specified, the source of the document will be returned.
     */
    public InnerHitBuilder setStoredFieldNames(List<String> fieldNames) {
        if (storedFieldsContext == null) {
            storedFieldsContext = StoredFieldsContext.fromList(fieldNames);
        } else {
            storedFieldsContext.addFieldNames(fieldNames);
        }
        return this;
    }

    /**
     * Gets the docvalue fields.
     */
    public List<FieldAndFormat> getDocValueFields() {
        return docValueFields;
    }

    /**
     * Sets the stored fields to load from the docvalue and return.
     */
    public InnerHitBuilder setDocValueFields(List<FieldAndFormat> docValueFields) {
        this.docValueFields = docValueFields;
        return this;
    }

    /**
     * Adds a field to load from the docvalue and return.
     */
    public InnerHitBuilder addDocValueField(String field, String format) {
        if (docValueFields == null) {
            docValueFields = new ArrayList<>();
        }
        docValueFields.add(new FieldAndFormat(field, null));
        return this;
    }

    /**
     * Adds a field to load from doc values and return.
     */
    public InnerHitBuilder addDocValueField(String field) {
        return addDocValueField(field, null);
    }

    public Set<ScriptField> getScriptFields() {
        return scriptFields;
    }

    public InnerHitBuilder setScriptFields(Set<ScriptField> scriptFields) {
        this.scriptFields = scriptFields;
        return this;
    }

    public InnerHitBuilder addScriptField(String name, Script script) {
        if (scriptFields == null) {
            scriptFields = new HashSet<>();
        }
        scriptFields.add(new ScriptField(name, script, false));
        return this;
    }

    public FetchSourceContext getFetchSourceContext() {
        return fetchSourceContext;
    }

    public InnerHitBuilder setFetchSourceContext(FetchSourceContext fetchSourceContext) {
        this.fetchSourceContext = fetchSourceContext;
        return this;
    }

    public List<SortBuilder<?>> getSorts() {
        return sorts;
    }

    public InnerHitBuilder setSorts(List<SortBuilder<?>> sorts) {
        this.sorts = sorts;
        return this;
    }

    public InnerHitBuilder addSort(SortBuilder<?> sort) {
        if (sorts == null) {
            sorts = new ArrayList<>();
        }
        sorts.add(sort);
        return this;
    }

    public HighlightBuilder getHighlightBuilder() {
        return highlightBuilder;
    }

    public InnerHitBuilder setHighlightBuilder(HighlightBuilder highlightBuilder) {
        this.highlightBuilder = highlightBuilder;
        return this;
    }

    QueryBuilder getQuery() {
        return query;
    }

    public InnerHitBuilder setInnerCollapse(CollapseBuilder innerCollapseBuilder) {
        this.innerCollapseBuilder = innerCollapseBuilder;
        return this;
    }

    public CollapseBuilder getInnerCollapseBuilder() {
        return innerCollapseBuilder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (name != null) {
            builder.field(NAME_FIELD.getPreferredName(), name);
        }
        builder.field(IGNORE_UNMAPPED.getPreferredName(), ignoreUnmapped);
        builder.field(SearchSourceBuilder.FROM_FIELD.getPreferredName(), from);
        builder.field(SearchSourceBuilder.SIZE_FIELD.getPreferredName(), size);
        builder.field(SearchSourceBuilder.VERSION_FIELD.getPreferredName(), version);
        builder.field(SearchSourceBuilder.EXPLAIN_FIELD.getPreferredName(), explain);
        builder.field(SearchSourceBuilder.TRACK_SCORES_FIELD.getPreferredName(), trackScores);
        if (fetchSourceContext != null) {
            builder.field(SearchSourceBuilder._SOURCE_FIELD.getPreferredName(), fetchSourceContext, params);
        }
        if (storedFieldsContext != null) {
            storedFieldsContext.toXContent(SearchSourceBuilder.STORED_FIELDS_FIELD.getPreferredName(), builder);
        }
        if (docValueFields != null) {
            builder.startArray(SearchSourceBuilder.DOCVALUE_FIELDS_FIELD.getPreferredName());
            for (FieldAndFormat docValueField : docValueFields) {
                if (docValueField.format == null) {
                    builder.value(docValueField.field);
                } else {
                    builder.startObject()
                        .field("field", docValueField.field)
                        .field("format", docValueField.format)
                        .endObject();
                }
            }
            builder.endArray();
        }
        if (scriptFields != null) {
            builder.startObject(SearchSourceBuilder.SCRIPT_FIELDS_FIELD.getPreferredName());
            for (ScriptField scriptField : scriptFields) {
                scriptField.toXContent(builder, params);
            }
            builder.endObject();
        }
        if (sorts != null) {
            builder.startArray(SearchSourceBuilder.SORT_FIELD.getPreferredName());
            for (SortBuilder<?> sort : sorts) {
                sort.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (highlightBuilder != null) {
            builder.field(SearchSourceBuilder.HIGHLIGHT_FIELD.getPreferredName(), highlightBuilder, params);
        }
        if (innerCollapseBuilder != null) {
            builder.field(COLLAPSE_FIELD.getPreferredName(), innerCollapseBuilder);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InnerHitBuilder that = (InnerHitBuilder) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(ignoreUnmapped, that.ignoreUnmapped) &&
                Objects.equals(from, that.from) &&
                Objects.equals(size, that.size) &&
                Objects.equals(explain, that.explain) &&
                Objects.equals(version, that.version) &&
                Objects.equals(trackScores, that.trackScores) &&
                Objects.equals(storedFieldsContext, that.storedFieldsContext) &&
                Objects.equals(docValueFields, that.docValueFields) &&
                Objects.equals(scriptFields, that.scriptFields) &&
                Objects.equals(fetchSourceContext, that.fetchSourceContext) &&
                Objects.equals(sorts, that.sorts) &&
                Objects.equals(highlightBuilder, that.highlightBuilder) &&
                Objects.equals(innerCollapseBuilder, that.innerCollapseBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, ignoreUnmapped, from, size, explain, version, trackScores,
                storedFieldsContext, docValueFields, scriptFields, fetchSourceContext, sorts, highlightBuilder, innerCollapseBuilder);
    }

    public static InnerHitBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, new InnerHitBuilder(), null);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
