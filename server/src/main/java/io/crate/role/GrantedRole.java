/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.role;

import java.io.IOException;
import java.util.Objects;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

public record GrantedRole(String roleName, String grantor) implements ToXContent, Writeable {

    public GrantedRole(StreamInput in) throws IOException {
        this(in.readString(), in.readString());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("role", roleName);
        builder.field("grantor", grantor);
        builder.endObject();

        return builder;
    }

    public static GrantedRole fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException(
                "failed to parse a granted role, expecting the current token to be a start_object, got " +
                    parser.currentToken()

            );
        }
        String role = null;
        String grantor = null;
        while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
            switch (parser.currentName()) {
                case "role":
                    parser.nextToken();
                    role = parser.text();
                    break;
                case "grantor":
                    parser.nextToken();
                    grantor = parser.text();
                    break;
                default:
                    break;
            }
        }
        if (parser.currentToken() != XContentParser.Token.END_OBJECT) {
            throw new ElasticsearchParseException(
                "failed to parse a granted role, expecting the current token to be a end_object, got " +
                    parser.currentToken()

            );
        }
        return new GrantedRole(role, grantor);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(roleName);
        out.writeString(grantor);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GrantedRole that = (GrantedRole) o;
        return Objects.equals(roleName, that.roleName) && Objects.equals(grantor, that.grantor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(roleName, grantor);
    }

    @Override
    public String toString() {
        return "GrantedRole{" + roleName + ", grantor: " + grantor + '}';
    }
}
