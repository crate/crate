/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.license;

import io.crate.license.exception.InvalidLicenseException;
import io.crate.license.exception.LicenseMetadataParsingException;
import org.elasticsearch.Version;
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

    static final int VERSION = 2;

    // limit the maximum license content number of bytes (this can vary based on the algorithm used for encryption and
    // the length of the client's name the license is issued to
    static final int MAX_LICENSE_CONTENT_LENGTH = 256;

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

    public static License decode(LicenseKey licenseKey) throws IOException {
        byte[] keyBytes = Base64.getDecoder().decode(licenseKey.licenseKey());
        License.Type type;
        ByteBuffer byteBuffer;
        int version;
        int contentLength;
        try {
            byteBuffer = ByteBuffer.wrap(keyBytes);
            type = License.Type.of(byteBuffer.getInt());
            version = byteBuffer.getInt();
            contentLength = byteBuffer.getInt();
        } catch (InvalidLicenseException e) {
            throw e;
        } catch (Exception e) {
            throw new InvalidLicenseException("The provided license key has an invalid format", e);
        }
        if (contentLength > MAX_LICENSE_CONTENT_LENGTH) {
            throw new InvalidLicenseException("The provided license key exceeds the maximum length of " + MAX_LICENSE_CONTENT_LENGTH);
        }
        byte[] contentBytes = new byte[contentLength];
        byteBuffer.get(contentBytes);

        switch (type) {
            case ENTERPRISE:
                return new EnterpriseLicense(version, contentBytes);
            case TRIAL:
            default:
                return new TrialLicense(version, contentBytes);
        }
    }

    /**
     * Creates a LicenseKey by encoding the license information in the following structure:
     *
     *      base64Encode(licenseType, version, contentLength, content)
     *
     */
    static LicenseKey encode(License.Type type, int version, byte[] content) {
        byte[] bytes = new byte[3 * Integer.BYTES + content.length];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byteBuffer.putInt(type.value())
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

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.ES_V_6_1_4;
    }
}
