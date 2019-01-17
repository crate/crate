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

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.StoredScriptSource;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class GetStoredScriptResponse extends ActionResponse implements StatusToXContentObject {

    public static final ParseField _ID_PARSE_FIELD = new ParseField("_id");
    public static final ParseField FOUND_PARSE_FIELD = new ParseField("found");
    public static final ParseField SCRIPT = new ParseField("script");

    private static final ConstructingObjectParser<GetStoredScriptResponse, String> PARSER =
        new ConstructingObjectParser<>("GetStoredScriptResponse",
            true,
            (a, c) -> {
                String id = (String) a[0];
                boolean found = (Boolean)a[1];
                StoredScriptSource scriptSource = (StoredScriptSource)a[2];
                return found ? new GetStoredScriptResponse(id, scriptSource) : new GetStoredScriptResponse(id, null);
            });

    static {
        PARSER.declareField(constructorArg(), (p, c) -> p.text(),
            _ID_PARSE_FIELD, ObjectParser.ValueType.STRING);
        PARSER.declareField(constructorArg(), (p, c) -> p.booleanValue(),
            FOUND_PARSE_FIELD, ObjectParser.ValueType.BOOLEAN);
        PARSER.declareField(optionalConstructorArg(), (p, c) -> StoredScriptSource.fromXContent(p, true),
            SCRIPT, ObjectParser.ValueType.OBJECT);
    }

    private String id;
    private StoredScriptSource source;

    GetStoredScriptResponse() {
    }

    GetStoredScriptResponse(String id, StoredScriptSource source) {
        this.id = id;
        this.source = source;
    }

    public String getId() {
        return id;
    }

    /**
     * @return if a stored script and if not found <code>null</code>
     */
    public StoredScriptSource getSource() {
        return source;
    }

    @Override
    public RestStatus status() {
        return source != null ? RestStatus.OK : RestStatus.NOT_FOUND;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(_ID_PARSE_FIELD.getPreferredName(), id);
        builder.field(FOUND_PARSE_FIELD.getPreferredName(), source != null);
        if (source != null) {
            builder.field(StoredScriptSource.SCRIPT_PARSE_FIELD.getPreferredName());
            source.toXContent(builder, params);
        }

        builder.endObject();
        return builder;
    }

    public static GetStoredScriptResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        if (in.readBoolean()) {
            if (in.getVersion().onOrAfter(Version.V_5_3_0)) {
                source = new StoredScriptSource(in);
            } else {
                source = new StoredScriptSource(in.readString());
            }
        } else {
            source = null;
        }

        if (in.getVersion().onOrAfter(Version.V_6_4_0)) {
            id = in.readString();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        if (source == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);

            if (out.getVersion().onOrAfter(Version.V_5_3_0)) {
                source.writeTo(out);
            } else {
                out.writeString(source.getSource());
            }
        }
        if (out.getVersion().onOrAfter(Version.V_6_4_0)) {
            out.writeString(id);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetStoredScriptResponse that = (GetStoredScriptResponse) o;
        return Objects.equals(id, that.id) &&
            Objects.equals(source, that.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, source);
    }
}
