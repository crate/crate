/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.license;

import io.crate.license.exception.LicenseMetadataParsingException;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Objects;

public class LicenseMetaData extends AbstractNamedDiffable<MetaData.Custom> implements MetaData.Custom {

    public static final String TYPE = "license";

    private long expirationDateInMs;
    private String issuedTo;
    private String signature;

    public LicenseMetaData(long expirationDateInMs, String issuedTo, String signature) {
        this.expirationDateInMs = expirationDateInMs;
        this.issuedTo = issuedTo;
        this.signature = signature;
    }

    public LicenseMetaData(StreamInput in) throws IOException {
        readFrom(in);
    }

    public long expirationDateInMs() {
        return expirationDateInMs;
    }

    public String issuedTo() {
        return issuedTo;
    }

    public String signature() {
        return signature;
    }

    public void readFrom(StreamInput in) throws IOException {
        this.expirationDateInMs = in.readLong();
        this.issuedTo = in.readString();
        this.signature = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(expirationDateInMs);
        out.writeString(issuedTo);
        out.writeString(signature);
    }

    /*
     * LicenseMetaData XContent has the following structure:
     *
     * <pre>
     *     {
     *       "license": {
     *           "expirationDateInMs": 202020202,
     *           "issuedTo": "organisation",
     *           "signture": "XXX"
     *       }
     *     }
     * </pre>
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .startObject(TYPE)
                .field("expirationDateInMs", expirationDateInMs)
                .field("issuedTo", issuedTo)
                .field("signature", signature)
            .endObject();
    }

    public static LicenseMetaData fromXContent(XContentParser parser) throws IOException {
        long expirationDateInMs = 0L;
        String issuedTo = null;
        String signature = null;
        // advance from metadata START_OBJECT
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME || !Objects.equals(parser.currentName(), TYPE)) {
            throw new LicenseMetadataParsingException("license FIELD_NAME expected but got " + parser.currentToken());
        }
        // advance to license START_OBJECT
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new LicenseMetadataParsingException("license START_OBJECT expected but got " + parser.currentToken());
        }

        while ((token = parser.nextToken()) == XContentParser.Token.FIELD_NAME) {
            if ("expirationDateInMs".equals(parser.currentName())) {
                expirationDateInMs = parseLongField(parser);
            } else if ("issuedTo".equals(parser.currentName())) {
                issuedTo = parseStringField(parser);
            } else if ("signature".equals(parser.currentName())) {
                signature = parseStringField(parser);
            } else {
                throw new LicenseMetadataParsingException("unexpected FIELD_NAME " + parser.currentToken());
            }
        }
        // license END_OBJECT is already consumed - check for correctness
        if (parser.currentToken() != XContentParser.Token.END_OBJECT) {
            throw new LicenseMetadataParsingException("expected the license object token at the end");

        }
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            // each custom metadata is packed inside an object.
            // each custom must move the parser to the end otherwise possible following customs won't be read
            throw new LicenseMetadataParsingException("expected an object token at the end");
        }
        return new LicenseMetaData(expirationDateInMs, issuedTo, signature);
    }

    private static String parseStringField(XContentParser parser) throws IOException {
        parser.nextToken();
        return parser.textOrNull();
    }

    private static long parseLongField(XContentParser parser) throws IOException {
        parser.nextToken();
        return parser.longValue();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LicenseMetaData licenseMetaData = (LicenseMetaData) o;
        return expirationDateInMs == licenseMetaData.expirationDateInMs &&
               Objects.equals(issuedTo, licenseMetaData.issuedTo) &&
               Objects.equals(signature, licenseMetaData.signature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expirationDateInMs, issuedTo, signature);
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return EnumSet.of(MetaData.XContentContext.GATEWAY, MetaData.XContentContext.SNAPSHOT);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

}
