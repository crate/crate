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
package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * All the required context to pull a field from the doc values.
 */
public class DocValueFieldsContext {

    public static final String USE_DEFAULT_FORMAT = "use_field_mapping";

    /**
     * Wrapper around a field name and the format that should be used to
     * display values of this field.
     */
    public static final class FieldAndFormat implements Writeable {

        private static final ConstructingObjectParser<FieldAndFormat, Void> PARSER = new ConstructingObjectParser<>("script",
                a -> new FieldAndFormat((String) a[0], (String) a[1]));
        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("field"));
            PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), new ParseField("format"));
        }

        /**
         * Parse a {@link FieldAndFormat} from some {@link XContent}.
         */
        public static FieldAndFormat fromXContent(XContentParser parser) throws IOException {
            Token token = parser.currentToken();
            if (token.isValue()) {
                return new FieldAndFormat(parser.text(), null);
            } else {
                return PARSER.apply(parser, null);
            }
        }

        /** The name of the field. */
        public final String field;

        /** The format of the field, or {@code null} if defaults should be used. */
        public final String format;

        /** Sole constructor. */
        public FieldAndFormat(String field, @Nullable String format) {
            this.field = Objects.requireNonNull(field);
            this.format = format;
        }

        /** Serialization constructor. */
        public FieldAndFormat(StreamInput in) throws IOException {
            this.field = in.readString();
            if (in.getVersion().onOrAfter(Version.V_6_4_0)) {
                format = in.readOptionalString();
            } else {
                format = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(field);
            if (out.getVersion().onOrAfter(Version.V_6_4_0)) {
                out.writeOptionalString(format);
            }
        }

        @Override
        public int hashCode() {
            int h = field.hashCode();
            h = 31 * h + Objects.hashCode(format);
            return h;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            FieldAndFormat other = (FieldAndFormat) obj;
            return field.equals(other.field) && Objects.equals(format, other.format);
        }

    }

    private final List<FieldAndFormat> fields;

    public DocValueFieldsContext(List<FieldAndFormat> fields) {
        this.fields = fields;
    }

    /**
     * Returns the required docvalue fields
     */
    public List<FieldAndFormat> fields() {
        return this.fields;
    }
}
