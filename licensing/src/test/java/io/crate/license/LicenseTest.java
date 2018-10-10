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

import static org.hamcrest.Matchers.nullValue;

public class LicenseTest extends CrateUnitTest {

    private static final String LICENSE_KEY = "ThisShouldBeAnEncryptedLicenseKey";

    public static LicenseKey createMetaData() {
        return new LicenseKey(LICENSE_KEY);
    }

    @Test
    public void testLicenseMetaDataStreaming() throws IOException {
        BytesStreamOutput stream = new BytesStreamOutput();
        LicenseKey licenseKey = createMetaData();
        licenseKey.writeTo(stream);

        StreamInput in = stream.bytes().streamInput();
        LicenseKey licenseKey2 = new LicenseKey(in);
        assertEquals(licenseKey, licenseKey2);
    }

    @Test
    public void testLicenceMetaDataToXContent() throws IOException {
        LicenseKey licenseKey = createMetaData();
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
    public void testLicenceMetaDataFromXContent() throws IOException {
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
        assertEquals(createMetaData(), licenseKey2);
        // a metadata custom must consume the surrounded END_OBJECT token, no token must be left
        assertThat(parser.nextToken(), nullValue());
    }
}
