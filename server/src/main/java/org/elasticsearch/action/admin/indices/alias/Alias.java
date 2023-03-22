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

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;

/**
 * Represents an alias, to be associated with an index
 */
public class Alias implements Writeable {

    private static final ParseField FILTER = new ParseField("filter");

    private String name;

    public Alias(String name) {
        this.name = name;
    }

    /**
     * Returns the alias name
     */
    public String name() {
        return name;
    }

    public Alias(StreamInput in) throws IOException {
        name = in.readString();
        if (in.getVersion().before(Version.V_5_3_0)) {
            in.readOptionalString(); // filter
            in.readOptionalString(); // indexRouting
            in.readOptionalString(); // searchRouting
            in.readOptionalBoolean(); // writeIndex
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        if (out.getVersion().before(Version.V_5_3_0)) {
            out.writeOptionalString(null);
            out.writeOptionalString(null);
            out.writeOptionalString(null);
            out.writeOptionalBoolean(null);
        }
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
                    parser.mapOrdered();
                }
            }
        }
        return alias;
    }

    @Override
    public String toString() {
        return "Alias{" + "name='" + name + "'}";
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
