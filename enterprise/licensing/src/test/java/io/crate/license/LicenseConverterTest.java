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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertThat;

public class LicenseConverterTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    static byte[] createV1JsonLicense(long expiryDateInMs, String issuedTo) {
        try {
            XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            contentBuilder.startObject()
                .field(LicenseConverter.EXPIRY_DATE_IN_MS, expiryDateInMs)
                .field(LicenseConverter.ISSUED_TO, issuedTo)
                .endObject();
            return Strings.toString(contentBuilder).getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void testLicenseDataSerializationAndDeserialization() throws IOException {
        final long expiryDate = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
        final int numNodes = 3;
        LicenseData licenseData = new LicenseData(expiryDate, "crate", numNodes);
        byte[] data = LicenseConverter.toJson(licenseData);
        LicenseData licenseDataFromByteArray =
            LicenseConverter.fromJson(data, 2);
        assertThat(licenseDataFromByteArray.expiryDateInMs(), Matchers.is(expiryDate));
        assertThat(licenseDataFromByteArray.issuedTo(), Matchers.is("crate"));
        assertThat(licenseDataFromByteArray.maxNumberOfNodes(), Matchers.is(numNodes));
    }

    @Test
    public void testThatDeserializationOfV2LicenseNoMaxNodesThrowsException() throws IOException {
        byte[] data = createV1JsonLicense(Long.MAX_VALUE, "crate");

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("LicenseKey v2 should have a valid value for maxNumberOfNodes");
        LicenseConverter.fromJson(data, 2);
    }

    @Test
    public void testDeserializationFromV1SerialisedData() throws IOException {
        final long expiryDate = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
        byte[] data = createV1JsonLicense(expiryDate, "crate");
        LicenseData licenseDataFromByteArray =
            LicenseConverter.fromJson(data, 1);
        assertThat(licenseDataFromByteArray.expiryDateInMs(), Matchers.is(expiryDate));
        assertThat(licenseDataFromByteArray.issuedTo(), Matchers.is("crate"));
        assertThat(licenseDataFromByteArray.maxNumberOfNodes(), Matchers.is(EnterpriseLicenseService.MAX_NODES_FOR_V1_LICENSES));
    }
}
