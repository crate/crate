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
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class LicenseKeyTest extends CrateUnitTest {

    @Test
    public void createLicenseKey() {
        LicenseKey licenseKey =
            LicenseKey.createLicenseKey(
                LicenseKey.SELF_GENERATED,
                LicenseKey.VERSION,
                "testLicense".getBytes(StandardCharsets.UTF_8));
        assertThat(licenseKey, is(notNullValue()));
    }

    @Test
    public void decodeLicense() {
        DecodedLicense decodedLicense =
            LicenseKey.decodeLicense(new LicenseKey("AAAAAAAAAAEAAABACYK5Ua3JBI98IJ99P/AsXCsV7UpHiBzSjkg+pFNDkpYAZUttlnqldjF5BAtRfzuJHA+2091XDmHACmF+M1J0NQ=="));

        assertThat(decodedLicense, is(notNullValue()));
        assertThat(decodedLicense.type(), is(LicenseKey.SELF_GENERATED));
        assertThat(decodedLicense.version(), is(1));
    }

    @Test
    public void decodeTooLongLicenseRaisesException() {
        byte[] largeContent = new byte[257];
        IntStream.range(0, 257).forEach(i -> largeContent[i] = 15);

        expectedException.expect(InvalidLicenseException.class);
        expectedException.expectMessage("The provided license key exceeds the maximum length of 256");
        LicenseKey.decodeLicense(new LicenseKey(new String(Base64.getEncoder().encode(largeContent))));
    }
}
