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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class DecryptedLicenseData {

    static final long UNLIMITED_EXPIRY_DATE_IN_MS = Long.MAX_VALUE;
    static final int MAX_NODES_FOR_V1_LICENSES = 10;
    static final int MAX_NODES_FOR_V2_LICENSES = 3;

    static final String EXPIRY_DATE_IN_MS = "expiryDateInMs";
    static final String ISSUED_TO = "issuedTo";
    private static final String MAX_NUMBER_OF_NODES = "maxNumberOfNodes";

    private final long expiryDateInMs;
    private final String issuedTo;
    private final int maxNumberOfNodes;

    public DecryptedLicenseData(long expiryDateInMs, String issuedTo, int maxNumberOfNodes) {
        this.expiryDateInMs = expiryDateInMs;
        this.issuedTo = issuedTo;
        this.maxNumberOfNodes = maxNumberOfNodes;
    }

    public long expiryDateInMs() {
        return expiryDateInMs;
    }

    public long millisToExpiration() {
        return expiryDateInMs - System.currentTimeMillis();
    }

    public String issuedTo() {
        return issuedTo;
    }

    public int maxNumberOfNodes() {
        return maxNumberOfNodes;
    }

    boolean isExpired() {
        return millisToExpiration() < 0;
    }

    /*
     * Creates the json representation of the license information with the following structure:
     *
     * <pre>
     *      {
     *          "expiryDateInMs": "XXX",
     *          "issuedTo": "YYY"
     *          "maxNumberOfNodes": "ZZ"
     *      }
     * </pre>
     */
    byte[] formatLicenseData() {
        try {
            XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            contentBuilder.startObject()
                .field(EXPIRY_DATE_IN_MS, expiryDateInMs)
                .field(ISSUED_TO, issuedTo)
                .field(MAX_NUMBER_OF_NODES, maxNumberOfNodes)
                .endObject();
            return Strings.toString(contentBuilder).getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static DecryptedLicenseData fromFormattedLicenseData(byte[] licenseData,
                                                         int version) throws IOException {
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, licenseData)) {
            XContentParser.Token token;
            long expiryDate = UNLIMITED_EXPIRY_DATE_IN_MS;
            String issuedTo = null;
            int maxNumberOfNodes = -1;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String currentFieldName = parser.currentName();
                    parser.nextToken();
                    if (currentFieldName.equals(EXPIRY_DATE_IN_MS)) {
                        expiryDate = parser.longValue();
                    } else if (currentFieldName.equals(ISSUED_TO)) {
                        issuedTo = parser.text();
                    } else if (currentFieldName.equals(MAX_NUMBER_OF_NODES)) {
                        maxNumberOfNodes = parser.intValue();
                    }
                }
            }
            if (maxNumberOfNodes == -1) {
                if (version == 1) {
                    maxNumberOfNodes = MAX_NODES_FOR_V1_LICENSES;
                } else if (version == 2) {
                    throw new IllegalStateException("LicenseKey v2 should have a valid value for " + MAX_NUMBER_OF_NODES);
                }
            }
            return new DecryptedLicenseData(expiryDate, issuedTo, maxNumberOfNodes);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DecryptedLicenseData that = (DecryptedLicenseData) o;
        return expiryDateInMs == that.expiryDateInMs &&
               maxNumberOfNodes == that.maxNumberOfNodes &&
               Objects.equals(issuedTo, that.issuedTo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expiryDateInMs, issuedTo, maxNumberOfNodes);
    }
}
