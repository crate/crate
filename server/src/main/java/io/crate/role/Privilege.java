/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import org.jetbrains.annotations.Nullable;

public class Privilege implements Writeable, ToXContent {

    /**
     * A Privilege is stored in the form of:
     * <p>
     *   {"policy": 1, "permission": 2, "securable": 3, "ident": "some_table/schema", "grantor": "grantor_username"}
     */
    public static Privilege fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException(
                "failed to parse privilege, expected a START_OBJECT token but got [{}]",
                parser.currentToken());
        }

        XContentParser.Token currentToken;
        Policy policy = null;
        Permission permission = null;
        Securable securable = null;
        String ident = null;
        String grantor = null;
        while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (currentToken == XContentParser.Token.FIELD_NAME) {
                String currentFieldName = parser.currentName();
                currentToken = parser.nextToken();
                switch (currentFieldName) {
                    case "policy", "state":
                        if (currentToken != XContentParser.Token.VALUE_NUMBER) {
                            throw new ElasticsearchParseException(
                                "failed to parse privilege, 'state' value is not a number [{}]", currentToken);
                        }
                        policy = Policy.VALUES.get(parser.intValue());
                        break;
                    case "permission", "type":
                        if (currentToken != XContentParser.Token.VALUE_NUMBER) {
                            throw new ElasticsearchParseException(
                                "failed to parse privilege, 'permission' value is not a number [{}]", currentToken);
                        }
                        permission = Permission.VALUES.get(parser.intValue());
                        break;
                    case "securable", "class":
                        if (currentToken != XContentParser.Token.VALUE_NUMBER) {
                            throw new ElasticsearchParseException(
                                "failed to parse privilege, 'securable' value is not a number [{}]", currentToken);
                        }
                        securable = Securable.VALUES.get(parser.intValue());
                        break;
                    case "ident":
                        if (currentToken != XContentParser.Token.VALUE_STRING
                            && currentToken != XContentParser.Token.VALUE_NULL) {
                            throw new ElasticsearchParseException(
                                "failed to parse privilege, 'ident' value is not a string or null [{}]", currentToken);
                        }
                        ident = parser.textOrNull();
                        break;
                    case "grantor":
                        if (currentToken != XContentParser.Token.VALUE_STRING) {
                            throw new ElasticsearchParseException(
                                "failed to parse privilege, 'grantor' value is not a string [{}]", currentToken);
                        }
                        grantor = parser.text();
                        break;
                    default:
                        throw new ElasticsearchParseException("failed to parse privilege");
                }
            }
        }
        return new Privilege(policy, permission, securable, ident, grantor);
    }

    private final Policy policy;
    private final PrivilegeIdent ident;
    private final String grantor;

    public Privilege(Policy policy,
                     Permission permission,
                     Securable securable,
                     @Nullable String ident,
                     String grantor) {
        this.policy = policy;
        this.ident = new PrivilegeIdent(permission, securable, ident);
        this.grantor = grantor;
    }

    public Privilege(StreamInput in) throws IOException {
        policy = in.readEnum(Policy.class);
        ident = new PrivilegeIdent(in);
        grantor = in.readString();
    }

    public Policy policy() {
        return policy;
    }

    public PrivilegeIdent ident() {
        return ident;
    }

    public String grantor() {
        return grantor;
    }

    /**
     * Equality validation.
     * <p>grantor</p> is left out by intend, this is just an information variable.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Privilege privilege = (Privilege) o;
        return policy == privilege.policy &&
               Objects.equals(ident, privilege.ident);
    }

    /**
     * Builds a hash code.
     * <p>grantor</p> is left out by intend, this is just an information variable.
     */
    @Override
    public int hashCode() {
        return Objects.hash(policy, ident);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(policy);
        ident.writeTo(out);
        out.writeString(grantor);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("policy", policy.ordinal())
            .field("permission", ident.permission().ordinal())
            .field("securable", ident.securable().ordinal())
            .field("ident", ident.ident())
            .field("grantor", grantor)
            .endObject();
    }
}
