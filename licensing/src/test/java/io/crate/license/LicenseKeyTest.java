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
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
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

import static io.crate.license.LicenseKey.LicenseType;
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
    public void testLicenceKeyToXContent() throws IOException {
        LicenseKey licenseKey = createLicenseKey();
        XContentBuilder builder = XContentFactory.jsonBuilder();

        // reflects the logic used to process custom metadata in the cluster state
        builder.startObject();
        licenseKey.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(xContentRegistry(), builder.bytes());
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

        XContentParser parser = JsonXContent.jsonXContent.createParser(xContentRegistry(), builder.bytes());
        parser.nextToken(); // start object
        LicenseKey licenseKey2 = LicenseKey.fromXContent(parser);
        assertEquals(createLicenseKey(), licenseKey2);
        // a metadata custom must consume the surrounded END_OBJECT token, no token must be left
        assertThat(parser.nextToken(), nullValue());
    }

    @Test
    public void testCreateLicenseKeyDoesNotProduceNullKey() {
        LicenseKey licenseKey =
            LicenseKey.createLicenseKey(
                LicenseType.TRIAL,
                LicenseKey.VERSION,
                "testLicense".getBytes(StandardCharsets.UTF_8));
        assertThat(licenseKey, is(notNullValue()));
    }

    @Test
    public void testCreateLicenseKeyInvalidLicenseTypeThrowsException() {
        expectedException.expect(InvalidLicenseException.class);
        expectedException.expectMessage("Invalid License Type");

        LicenseKey.createLicenseKey(LicenseType.of(-2), LicenseKey.VERSION, "testLicense".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testDecodeLicense() {
        DecodedLicense decodedLicense = LicenseKey.decodeLicense(createLicenseKey());

        assertThat(decodedLicense, is(notNullValue()));
        assertThat(decodedLicense.type(), is(LicenseType.TRIAL));
        assertThat(decodedLicense.version(), is(1));
    }

    @Test
    public void testDecodeTooLongLicenseRaisesException() {
        byte[] largeContent = new byte[LicenseKey.MAX_LICENSE_CONTENT_LENGTH + 1];
        IntStream.range(0, LicenseKey.MAX_LICENSE_CONTENT_LENGTH + 1).forEach(i -> largeContent[i] = 15);

        // adjust first bytes to match a valid license type
        ByteBuffer largeContentBuffer = ByteBuffer.wrap(largeContent);
        largeContentBuffer.putInt(LicenseType.TRIAL.value());

        expectedException.expect(InvalidLicenseException.class);
        expectedException.expectMessage("The provided license key exceeds the maximum length of " + LicenseKey.MAX_LICENSE_CONTENT_LENGTH);
        LicenseKey.decodeLicense(new LicenseKey(new String(Base64.getEncoder().encode(largeContentBuffer.array()))));
    }
}
