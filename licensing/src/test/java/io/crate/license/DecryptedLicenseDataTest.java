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

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class DecryptedLicenseDataTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testLicenseIsNotExpiredWhenExpirationDateIsInFuture() {
        DecryptedLicenseData nonExpiredLicenseData = new DecryptedLicenseData(
            System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1),
            "crate",
            3);
        assertThat(nonExpiredLicenseData.isExpired(), is(false));
    }

    @Test
    public void testLicenseIsExpiredWhenExpirationDateIsInPast() {
        DecryptedLicenseData expiredLicenseData = new DecryptedLicenseData(
            System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1),
            "crate",
            3);
        assertThat(expiredLicenseData.isExpired(), is(true));
    }

    @Test
    public void testDecryptedLicenseDataSerialisationAndDeserialisation() throws IOException {
        final long expiryDate = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
        final int numNodes = 3;
        DecryptedLicenseData licenseData = new DecryptedLicenseData(expiryDate, "crate", numNodes);
        byte[] data = licenseData.formatLicenseData();
        DecryptedLicenseData licenseDataFromByteArray =
            DecryptedLicenseData.fromFormattedLicenseData(data, 2);
        assertThat(licenseDataFromByteArray.expiryDateInMs(), Matchers.is(expiryDate));
        assertThat(licenseDataFromByteArray.issuedTo(), Matchers.is("crate"));
        assertThat(licenseDataFromByteArray.maxNumberOfNodes(), Matchers.is(numNodes));
    }

    @Test
    public void testThatDeserialisationOfV2LicenseNoMaxNodesThrowsException() throws IOException {
        byte[] data = DecryptedLicenseDataV1.formatLicenseData(Long.MAX_VALUE, "crate");

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("LicenseKey v2 should have a valid value for maxNumberOfNodes");
        DecryptedLicenseData.fromFormattedLicenseData(data, 2);
    }

    @Test
    public void testDeserialisationFromV1SerialisedData() throws IOException {
        final long expiryDate = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
        byte[] data = DecryptedLicenseDataV1.formatLicenseData(expiryDate, "crate");
        DecryptedLicenseData licenseDataFromByteArray =
            DecryptedLicenseData.fromFormattedLicenseData(data, 1);
        assertThat(licenseDataFromByteArray.expiryDateInMs(), Matchers.is(expiryDate));
        assertThat(licenseDataFromByteArray.issuedTo(), Matchers.is("crate"));
        assertThat(licenseDataFromByteArray.maxNumberOfNodes(), Matchers.is(DecryptedLicenseData.MAX_NODES_FOR_V1_LICENSES));
    }
}
