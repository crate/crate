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

import io.crate.es.common.Strings;
import io.crate.es.common.xcontent.DeprecationHandler;
import io.crate.es.common.xcontent.NamedXContentRegistry;
import io.crate.es.common.xcontent.XContentBuilder;
import io.crate.es.common.xcontent.XContentFactory;
import io.crate.es.common.xcontent.XContentParser;
import io.crate.es.common.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class DecryptedLicenseData {

    public static final String EXPIRY_DATE_IN_MS = "expiryDateInMs";
    public static final String ISSUED_TO = "issuedTo";

    private final long expiryDateInMs;
    private final String issuedTo;

    public DecryptedLicenseData(long expiryDateInMs, String issuedTo) {
        this.expiryDateInMs = expiryDateInMs;
        this.issuedTo = issuedTo;
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
     *      }
     * </pre>
     */
    byte[] formatLicenseData() {
        try {
            XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            contentBuilder.startObject()
                .field(EXPIRY_DATE_IN_MS, expiryDateInMs)
                .field(ISSUED_TO, issuedTo)
                .endObject();
            return Strings.toString(contentBuilder).getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static DecryptedLicenseData fromFormattedLicenseData(byte[] licenseData) throws IOException {
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, licenseData)) {
            XContentParser.Token token;
            long expiryDate = 0;
            String issuedTo = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String currentFieldName = parser.currentName();
                    parser.nextToken();
                    if (currentFieldName.equals(EXPIRY_DATE_IN_MS)) {
                        expiryDate = parser.longValue();
                    } else if (currentFieldName.equals(ISSUED_TO)) {
                        issuedTo = parser.text();
                    }
                }
            }
            return new DecryptedLicenseData(expiryDate, issuedTo);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DecryptedLicenseData that = (DecryptedLicenseData) o;
        return expiryDateInMs == that.expiryDateInMs &&
               Objects.equals(issuedTo, that.issuedTo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expiryDateInMs, issuedTo);
    }
}
