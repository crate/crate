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
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.stream.IntStream;

import static io.crate.license.License.Type;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class LicenseKeyTest extends CrateUnitTest {

    private static final String LICENSE_KEY = "AAAAAAAAAAEAAABACYK5Ua3JBI98IJ99P/AsXCsV7UpHiBzSjkg+pFNDkpYAZUttlnqldjF5BAtRfzuJHA+2091XDmHACmF+M1J0NQ==";

    public static LicenseKey createLicenseKey() {
        return new LicenseKey(LICENSE_KEY);
    }

    @Test
    public void testLicenseKeyStreaming() throws IOException {
        BytesStreamOutput stream = new BytesStreamOutput();
        LicenseKey licenseKey = createLicenseKey();
        licenseKey.writeTo(stream);

        StreamInput in = stream.bytes().streamInput();
        LicenseKey licenseKey2 = new LicenseKey(in);
        assertEquals(licenseKey, licenseKey2);
    }

    @Test
    public void testDecodeErrorOnFirstReadIntResultsInMeaningfulError() throws Exception {
        LicenseKey licenseKey = new LicenseKey("foo");
        expectedException.expectMessage("The provided license key has an invalid format");
        LicenseKey.decode(licenseKey);
    }

    @Test
    public void testLicenceKeyToXContent() throws IOException {
        LicenseKey licenseKey = createLicenseKey();
        XContentBuilder builder = XContentFactory.jsonBuilder();

        // reflects the logic used to process custom metadata in the cluster state
        builder.startObject();
        licenseKey.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            BytesReference.toBytes(BytesReference.bytes(builder)));
        parser.nextToken(); // start object
        LicenseKey licenseKey2 = LicenseKey.fromXContent(parser);
        assertEquals(licenseKey, licenseKey2);
        // a metadata custom must consume the surrounded END_OBJECT token, no token must be left
        assertThat(parser.nextToken(), nullValue());
    }

    @Test
    public void testLicenceKeyFromXContent() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        // reflects the logic used to process custom metadata in the cluster state
        builder.startObject();
        builder.startObject(LicenseKey.WRITEABLE_TYPE)
            .field("license_key", LICENSE_KEY)
            .endObject();
        builder.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            BytesReference.toBytes(BytesReference.bytes(builder)));
        parser.nextToken(); // start object
        LicenseKey licenseKey2 = LicenseKey.fromXContent(parser);
        assertEquals(createLicenseKey(), licenseKey2);
        // a metadata custom must consume the surrounded END_OBJECT token, no token must be left
        assertThat(parser.nextToken(), nullValue());
    }

    @Test
    public void testCreateLicenseKeyDoesNotProduceNullKey() {
        LicenseKey licenseKey =
            LicenseKey.encode(Type.TRIAL, LicenseKey.VERSION, "testLicense".getBytes(StandardCharsets.UTF_8));
        assertThat(licenseKey, is(notNullValue()));
    }

    @Test
    public void testCreateLicenseKeyInvalidLicenseTypeThrowsException() {
        expectedException.expect(InvalidLicenseException.class);
        expectedException.expectMessage("Invalid License Type");

        LicenseKey.encode(Type.of(-2), LicenseKey.VERSION, "testLicense".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testDecodeLicense() throws Exception {
        License decodedLicense = LicenseKey.decode(createLicenseKey());

        assertThat(decodedLicense, is(notNullValue()));
        assertThat(decodedLicense.type(), is(Type.TRIAL));
        assertThat(decodedLicense.version(), is(1));
    }

    @Test
    public void testDecodeTooLongLicenseRaisesException() throws Exception {
        byte[] largeContent = new byte[LicenseKey.MAX_LICENSE_CONTENT_LENGTH + 1];
        IntStream.range(0, LicenseKey.MAX_LICENSE_CONTENT_LENGTH + 1).forEach(i -> largeContent[i] = 15);

        // adjust first bytes to match a valid license type
        ByteBuffer largeContentBuffer = ByteBuffer.wrap(largeContent);
        largeContentBuffer.putInt(Type.TRIAL.value());

        expectedException.expect(InvalidLicenseException.class);
        expectedException.expectMessage("The provided license key exceeds the maximum length of " + LicenseKey.MAX_LICENSE_CONTENT_LENGTH);
        LicenseKey.decode(new LicenseKey(new String(Base64.getEncoder().encode(largeContentBuffer.array()))));
    }
}
