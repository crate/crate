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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static io.crate.license.EnterpriseLicenseService.MAX_NODES_FOR_V1_LICENSES;
import static io.crate.license.EnterpriseLicenseService.UNLIMITED_EXPIRY_DATE_IN_MS;

final class LicenseConverter {

    static final String EXPIRY_DATE_IN_MS = "expiryDateInMs";
    static final String ISSUED_TO = "issuedTo";
    private static final String MAX_NUMBER_OF_NODES = "maxNumberOfNodes";

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
    static byte[] toJson(LicenseData licenseData) {
        try {
            XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            contentBuilder.startObject()
                .field(EXPIRY_DATE_IN_MS, licenseData.expiryDateInMs())
                .field(ISSUED_TO, licenseData.issuedTo())
                .field(MAX_NUMBER_OF_NODES, licenseData.maxNumberOfNodes())
                .endObject();
            return Strings.toString(contentBuilder).getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static LicenseData fromJson(byte[] jsonBytes, int version) throws IOException {
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, jsonBytes)) {
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
                    throw new IllegalStateException("LicenseKey v2 should have a valid value for " +
                                                    MAX_NUMBER_OF_NODES);
                }
            }
            return new LicenseData(expiryDate, issuedTo, maxNumberOfNodes);
        }
    }
}
