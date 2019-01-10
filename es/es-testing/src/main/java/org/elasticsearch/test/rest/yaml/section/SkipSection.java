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
package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.rest.yaml.Features;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a skip section that tells whether a specific test section or suite needs to be skipped
 * based on:
 * - the elasticsearch version the tests are running against
 * - a specific test feature required that might not be implemented yet by the runner
 */
public class SkipSection {
    /**
     * Parse a {@link SkipSection} if the next field is {@code skip}, otherwise returns {@link SkipSection#EMPTY}.
     */
    public static SkipSection parseIfNext(XContentParser parser) throws IOException {
        ParserUtils.advanceToFieldName(parser);

        if ("skip".equals(parser.currentName())) {
            SkipSection section = parse(parser);
            parser.nextToken();
            return section;
        }

        return EMPTY;
    }

    public static SkipSection parse(XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Expected [" + XContentParser.Token.START_OBJECT +
                    ", found [" + parser.currentToken() + "], the skip section is not properly indented");
        }
        String currentFieldName = null;
        XContentParser.Token token;
        String version = null;
        String reason = null;
        List<String> features = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("version".equals(currentFieldName)) {
                    version = parser.text();
                } else if ("reason".equals(currentFieldName)) {
                    reason = parser.text();
                } else if ("features".equals(currentFieldName)) {
                    features.add(parser.text());
                }
                else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "field " + currentFieldName + " not supported within skip section");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("features".equals(currentFieldName)) {
                    while(parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        features.add(parser.text());
                    }
                }
            }
        }

        parser.nextToken();

        if (!Strings.hasLength(version) && features.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), "version or features is mandatory within skip section");
        }
        if (Strings.hasLength(version) && !Strings.hasLength(reason)) {
            throw new ParsingException(parser.getTokenLocation(), "reason is mandatory within skip version section");
        }
        return new SkipSection(version, features, reason);
    }

    public static final SkipSection EMPTY = new SkipSection();

    private final Version lowerVersion;
    private final Version upperVersion;
    private final List<String> features;
    private final String reason;

    private SkipSection() {
        this.lowerVersion = null;
        this.upperVersion = null;
        this.features = new ArrayList<>();
        this.reason = null;
    }

    public SkipSection(String versionRange, List<String> features, String reason) {
        assert features != null;
        Version[] versions = parseVersionRange(versionRange);
        this.lowerVersion = versions[0];
        this.upperVersion = versions[1];
        this.features = features;
        this.reason = reason;
    }

    public Version getLowerVersion() {
        return lowerVersion;
    }

    public Version getUpperVersion() {
        return upperVersion;
    }

    public List<String> getFeatures() {
        return features;
    }

    public String getReason() {
        return reason;
    }

    public boolean skip(Version currentVersion) {
        if (isEmpty()) {
            return false;
        }
        boolean skip = lowerVersion != null && upperVersion != null && currentVersion.onOrAfter(lowerVersion)
            && currentVersion.onOrBefore(upperVersion);
        skip |= Features.areAllSupported(features) == false;
        return skip;
    }

    public boolean isVersionCheck() {
        return features.isEmpty();
    }

    public boolean isEmpty() {
        return EMPTY.equals(this);
    }

    static Version[] parseVersionRange(String versionRange) {
        if (versionRange == null) {
            return new Version[] { null, null };
        }
        if (versionRange.trim().equals("all")) {
            return new Version[]{VersionUtils.getFirstVersion(), Version.CURRENT};
        }
        String[] skipVersions = versionRange.split("-");
        if (skipVersions.length > 2) {
            throw new IllegalArgumentException("version range malformed: " + versionRange);
        }

        String lower = skipVersions[0].trim();
        String upper = skipVersions[1].trim();
        return new Version[] {
            lower.isEmpty() ? VersionUtils.getFirstVersion() : Version.fromString(lower),
            upper.isEmpty() ? Version.CURRENT : Version.fromString(upper)
        };
    }

    public String getSkipMessage(String description) {
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append("[").append(description).append("] skipped,");
        if (reason != null) {
             messageBuilder.append(" reason: [").append(getReason()).append("]");
        }
        if (features.isEmpty() == false) {
            messageBuilder.append(" unsupported features ").append(getFeatures());
        }
        return messageBuilder.toString();
    }
}
