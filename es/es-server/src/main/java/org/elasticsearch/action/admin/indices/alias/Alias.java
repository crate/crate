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

package org.elasticsearch.action.admin.indices.alias;

import org.elasticsearch.ElasticsearchGenerationException;
import javax.annotation.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

/**
 * Represents an alias, to be associated with an index
 */
public class Alias implements Writeable {

    private static final ParseField FILTER = new ParseField("filter");
    private static final ParseField ROUTING = new ParseField("routing");
    private static final ParseField INDEX_ROUTING = new ParseField("index_routing", "indexRouting", "index-routing");
    private static final ParseField SEARCH_ROUTING = new ParseField("search_routing", "searchRouting", "search-routing");
    private static final ParseField IS_WRITE_INDEX = new ParseField("is_write_index");

    private String name;

    @Nullable
    private String filter;

    @Nullable
    private String indexRouting;

    @Nullable
    private String searchRouting;

    @Nullable
    private Boolean writeIndex;

    private Alias() {

    }

    public Alias(String name) {
        this.name = name;
    }

    /**
     * Returns the alias name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the filter associated with the alias
     */
    public String filter() {
        return filter;
    }

    /**
     * Associates a filter to the alias
     */
    public Alias filter(String filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Associates a filter to the alias
     */
    public Alias filter(Map<String, Object> filter) {
        if (filter == null || filter.isEmpty()) {
            this.filter = null;
            return this;
        }
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(filter);
            this.filter = Strings.toString(builder);
            return this;
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + filter + "]", e);
        }
    }

    /**
     * Associates a routing value to the alias
     */
    public Alias routing(String routing) {
        this.indexRouting = routing;
        this.searchRouting = routing;
        return this;
    }

    /**
     * Returns the index routing value associated with the alias
     */
    public String indexRouting() {
        return indexRouting;
    }

    /**
     * Associates an index routing value to the alias
     */
    public Alias indexRouting(String indexRouting) {
        this.indexRouting = indexRouting;
        return this;
    }

    /**
     * Returns the search routing value associated with the alias
     */
    public String searchRouting() {
        return searchRouting;
    }

    /**
     * Associates a search routing value to the alias
     */
    public Alias searchRouting(String searchRouting) {
        this.searchRouting = searchRouting;
        return this;
    }

    /**
     * @return the write index flag for the alias
     */
    public Boolean writeIndex() {
        return writeIndex;
    }

    /**
     *  Sets whether an alias is pointing to a write-index
     */
    public Alias writeIndex(@Nullable Boolean writeIndex) {
        this.writeIndex = writeIndex;
        return this;
    }

    public Alias(StreamInput in) throws IOException {
        name = in.readString();
        filter = in.readOptionalString();
        indexRouting = in.readOptionalString();
        searchRouting = in.readOptionalString();
        writeIndex = in.readOptionalBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeOptionalString(filter);
        out.writeOptionalString(indexRouting);
        out.writeOptionalString(searchRouting);
        out.writeOptionalBoolean(writeIndex);
    }

    /**
     * Parses an alias and returns its parsed representation
     */
    public static Alias fromXContent(XContentParser parser) throws IOException {
        Alias alias = new Alias(parser.currentName());

        String currentFieldName = null;
        XContentParser.Token token = parser.nextToken();
        if (token == null) {
            throw new IllegalArgumentException("No alias is specified");
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (FILTER.match(currentFieldName, parser.getDeprecationHandler())) {
                    Map<String, Object> filter = parser.mapOrdered();
                    alias.filter(filter);
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (ROUTING.match(currentFieldName, parser.getDeprecationHandler())) {
                    alias.routing(parser.text());
                } else if (INDEX_ROUTING.match(currentFieldName, parser.getDeprecationHandler())) {
                    alias.indexRouting(parser.text());
                } else if (SEARCH_ROUTING.match(currentFieldName, parser.getDeprecationHandler())) {
                    alias.searchRouting(parser.text());
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if (IS_WRITE_INDEX.match(currentFieldName, parser.getDeprecationHandler())) {
                    alias.writeIndex(parser.booleanValue());
                }
            }
        }
        return alias;
    }

    @Override
    public String toString() {
        return "Alias{" +
               "name='" + name + '\'' +
               ", filter='" + filter + '\'' +
               ", indexRouting='" + indexRouting + '\'' +
               ", searchRouting='" + searchRouting + '\'' +
               ", writeIndex=" + writeIndex +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Alias alias = (Alias) o;

        if (name != null ? !name.equals(alias.name) : alias.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }
}
