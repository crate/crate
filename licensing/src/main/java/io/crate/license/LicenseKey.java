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

import io.crate.license.exception.InvalidLicenseException;
import io.crate.license.exception.LicenseMetadataParsingException;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.EnumSet;
import java.util.Objects;

public class LicenseKey extends AbstractNamedDiffable<MetaData.Custom> implements MetaData.Custom {

    public static final String WRITEABLE_TYPE = "license";

    static final int TRIAL = 0;
    static final int VERSION = 1;

    // limit the maximum license content number of bytes (this can vary based on the algorithm used for encryption and
    // the length of the client's name the license is issued to
    private static final int MAX_LICENSE_CONTENT_LENGTH = 256;

    private String licenseKey;

    public LicenseKey(final String licenseKey) {
        this.licenseKey = licenseKey;
    }

    public LicenseKey(StreamInput in) throws IOException {
        readFrom(in);
    }

    String licenseKey() {
        return licenseKey;
    }

    public static DecodedLicense decodeLicense(LicenseKey licenseKey) {
        byte[] keyBytes = Base64.getDecoder().decode(licenseKey.licenseKey());
        ByteBuffer byteBuffer = ByteBuffer.wrap(keyBytes);
        int licenseType = byteBuffer.getInt();
        int version = byteBuffer.getInt();
        int contentLength = byteBuffer.getInt();
        if (contentLength > MAX_LICENSE_CONTENT_LENGTH) {
            throw new InvalidLicenseException("The provided license key exceeds the maximum length of " + MAX_LICENSE_CONTENT_LENGTH);
        }
        byte[] contentBytes = new byte[contentLength];
        byteBuffer.get(contentBytes);

        return new DecodedLicense(licenseType, version, contentBytes);
    }

    /**
     * Creates a LicenseKey by encoding the license information in the following structure:
     *
     *      base64Encode(licenseType, version, contentLength, content)
     *
     */
    static LicenseKey createLicenseKey(int licenseType, int version, byte[] content) {
        byte[] bytes = new byte[4 + 4 + 4 + content.length];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byteBuffer.putInt(licenseType)
            .putInt(version)
            .putInt(content.length)
            .put(content);
        return new LicenseKey(Base64.getEncoder().encodeToString(bytes));
    }

    public void readFrom(StreamInput in) throws IOException {
        this.licenseKey = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(licenseKey);
    }

    /*
     * LicenseMetaData XContent has the following structure:
     *
     * <pre>
     *     {
     *       "license": {
     *           "licenseKey": "XXX"
     *       }
     *     }
     * </pre>
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .startObject(WRITEABLE_TYPE)
                .field("license_key", licenseKey)
            .endObject();
    }

    public static LicenseKey fromXContent(XContentParser parser) throws IOException {
        String licenseKey = null;
        // advance from metadata START_OBJECT
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME || !Objects.equals(parser.currentName(), WRITEABLE_TYPE)) {
            throw new LicenseMetadataParsingException("license FIELD_NAME expected but got " + parser.currentToken());
        }
        // advance to license START_OBJECT
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new LicenseMetadataParsingException("license START_OBJECT expected but got " + parser.currentToken());
        }

        while ((token = parser.nextToken()) == XContentParser.Token.FIELD_NAME) {
            if ("license_key".equals(parser.currentName())) {
                licenseKey = parseStringField(parser);
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
        return new LicenseKey(licenseKey);
    }

    private static String parseStringField(XContentParser parser) throws IOException {
        parser.nextToken();
        return parser.textOrNull();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LicenseKey licenseKey = (LicenseKey) o;
        return Objects.equals(this.licenseKey, licenseKey.licenseKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(licenseKey);
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return EnumSet.of(MetaData.XContentContext.GATEWAY, MetaData.XContentContext.SNAPSHOT);
    }

    @Override
    public String getWriteableName() {
        return WRITEABLE_TYPE;
    }

}
