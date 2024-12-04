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

package io.crate.license;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;

/**
 * Kept for BWC so it can still be streamed when coming from a CrateDB < 4.5 node
 */
public class License extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String WRITEABLE_TYPE = "license";

    private String licenseKey;

    public License(final String licenseKey) {
        this.licenseKey = licenseKey;
    }

    public License(StreamInput in) throws IOException {
        readFrom(in);
    }

    public void readFrom(StreamInput in) throws IOException {
        this.licenseKey = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(licenseKey);
    }

    /*
     * LicenseMetadata XContent has the following structure:
     *
     * <pre>
     *     {
     *       "license": {
     *           "licenseKey": "XXX"
     *       }
     *     }
     * </pre>
     */
    public static License fromXContent(XContentParser parser) throws IOException {
        String licenseKey = null;
        // advance from metadata START_OBJECT
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME || !Objects.equals(parser.currentName(), WRITEABLE_TYPE)) {
            throw new IllegalArgumentException("license FIELD_NAME expected but got " + parser.currentToken());
        }
        // advance to license START_OBJECT
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("license START_OBJECT expected but got " + parser.currentToken());
        }

        while ((token = parser.nextToken()) == XContentParser.Token.FIELD_NAME) {
            if ("license_key".equals(parser.currentName())) {
                licenseKey = parseStringField(parser);
            } else {
                throw new IllegalArgumentException("unexpected FIELD_NAME " + parser.currentToken());
            }
        }
        // license END_OBJECT is already consumed - check for correctness
        if (parser.currentToken() != XContentParser.Token.END_OBJECT) {
            throw new IllegalArgumentException("expected the license object token at the end");

        }
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            // each custom metadata is packed inside an object.
            // each custom must move the parser to the end otherwise possible following customs won't be read
            throw new IllegalArgumentException("expected an object token at the end");
        }
        return new License(licenseKey);
    }

    private static String parseStringField(XContentParser parser) throws IOException {
        parser.nextToken();
        return parser.textOrNull();
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedWriteableRegistry.Entry(
            Metadata.Custom.class,
            License.WRITEABLE_TYPE,
            License::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            License.WRITEABLE_TYPE,
            in -> License.readDiffFrom(Metadata.Custom.class, License.WRITEABLE_TYPE, in)
        ));
        return entries;
    }

    public static List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Collections.singletonList(new NamedXContentRegistry.Entry(
            Metadata.Custom.class,
            new ParseField(License.WRITEABLE_TYPE),
            License::fromXContent
        ));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        License licenseKey = (License) o;
        return Objects.equals(this.licenseKey, licenseKey.licenseKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(licenseKey);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT);
    }

    @Override
    public String getWriteableName() {
        return WRITEABLE_TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_3_0_1;
    }
}
