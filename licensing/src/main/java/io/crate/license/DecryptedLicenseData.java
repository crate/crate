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

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class DecryptedLicenseData {

    public static final String EXPIRATION_DATE_IN_MS = "expirationDateInMs";
    public static final String ISSUED_TO = "issuedTo";

    private final long expirationDateInMs;
    private final String issuedTo;

    public DecryptedLicenseData(long expirationDateInMs, String issuedTo) {
        this.expirationDateInMs = expirationDateInMs;
        this.issuedTo = issuedTo;
    }

    public long expirationDateInMs() {
        return expirationDateInMs;
    }

    public long millisToExpiration() {
        return expirationDateInMs - System.currentTimeMillis();
    }

    public String issuedTo() {
        return issuedTo;
    }

    /*
     * Creates the json representation of the license information with the following structure:
     *
     * <pre>
     *      {
     *          "expirationDateInMs": "XXX",
     *          "issuedTo": "YYY"
     *      }
     * </pre>
     */
    byte[] formatLicenseData() {
        try {
            XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            contentBuilder.startObject()
                .field(EXPIRATION_DATE_IN_MS, expirationDateInMs)
                .field(ISSUED_TO, issuedTo)
                .endObject();
            return contentBuilder.string().getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static DecryptedLicenseData fromFormattedLicenseData(byte[] licenseData) throws IOException {
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(NamedXContentRegistry.EMPTY, licenseData)) {
            XContentParser.Token token;
            long expirationDate = 0;
            String issuedTo = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String currentFieldName = parser.currentName();
                    parser.nextToken();
                    if (currentFieldName.equals(EXPIRATION_DATE_IN_MS)) {
                        expirationDate = parser.longValue();
                    } else if (currentFieldName.equals(ISSUED_TO)) {
                        issuedTo = parser.text();
                    }
                }
            }
            return new DecryptedLicenseData(expirationDate, issuedTo);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DecryptedLicenseData that = (DecryptedLicenseData) o;
        return expirationDateInMs == that.expirationDateInMs &&
               Objects.equals(issuedTo, that.issuedTo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expirationDateInMs, issuedTo);
    }
}
