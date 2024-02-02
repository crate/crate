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
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Maps;


/**
 * Represents JWT token payload.
 * @param iss https://datatracker.ietf.org/doc/html/rfc7519#section-4.1.1
 * @param username is username on the third party app. Not necessarily same as CrateDB user.
 */
public record JwtProperties(String iss, String username) implements Writeable, ToXContent {

    public static JwtProperties readFrom(StreamInput in) throws IOException {
        return new JwtProperties(in.readString(), in.readString());
    }

    @Nullable
    public static JwtProperties fromMap(@NotNull Map<String, Object> jwtPropertiesMap) {
        if (jwtPropertiesMap.isEmpty() == false) {
            String iss = Maps.get(jwtPropertiesMap, "iss");
            ensureNotNull("iss", iss);
            String username = Maps.get(jwtPropertiesMap, "username");
            ensureNotNull("username", username);
            if (jwtPropertiesMap.size() > 2) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Only 'iss' and 'username' JWT properties are allowed")
                );
            }
            return new JwtProperties(iss, username);
        }
        return null;

    }

    private static void ensureNotNull(String propertyName, @Nullable String value) {
        if (value == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "JWT property '%s' must have a non-null value", propertyName)
            );
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(iss);
        out.writeString(username);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("jwt")
            .field("iss", iss)
            .field("username", username)
            .endObject();
        return builder;
    }

    public static JwtProperties fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token currentToken;
        String iss = null;
        String username = null;
        while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (currentToken == XContentParser.Token.FIELD_NAME) {
                String currentFieldName = parser.currentName();
                currentToken = parser.nextToken();
                switch (currentFieldName) {
                    case "iss":
                        if (currentToken != XContentParser.Token.VALUE_STRING) {
                            throw new ElasticsearchParseException(
                                "failed to parse jwt, 'iss' value is not a string [{}]", currentToken);
                        }
                        iss = parser.text();
                        break;
                    case "username":
                        if (currentToken != XContentParser.Token.VALUE_STRING) {
                            throw new ElasticsearchParseException(
                                "failed to parse jwt, 'username' value is not a string [{}]", currentToken);
                        }
                        username = parser.text();
                        break;
                    default:
                        throw new ElasticsearchParseException("failed to parse jwt, unknown property '{}'", currentFieldName);
                }
            }
        }
        return new JwtProperties(iss, username);
    }
}
