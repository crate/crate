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

package io.crate.expression.reference.sys.cluster;

import io.crate.license.LicenseData;
import io.crate.license.LicenseService;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static io.crate.expression.reference.sys.cluster.ClusterLicenseExpression.EXPIRY_DATE;
import static io.crate.expression.reference.sys.cluster.ClusterLicenseExpression.ISSUED_TO;
import static io.crate.expression.reference.sys.cluster.ClusterLicenseExpression.MAX_NODES;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterLicenseExpressionTest extends CrateUnitTest {

    private LicenseService licenseService;
    private ClusterLicenseExpression licenseExpression;

    @Before
    public void setupClusterLicenseExpression() {
        licenseService = mock(LicenseService.class);
        licenseExpression = new ClusterLicenseExpression(licenseService);
    }

    @Test
    public void testExpressionValueReturnsNullForMissingLicense() {
        when(licenseService.currentLicense()).thenReturn(null);
        assertThat(licenseExpression.value(), is(nullValue()));
    }

    @Test
    public void testChildValueReturnsNullForMissingLicense() {
        when(licenseService.currentLicense()).thenReturn(null);
        assertThat(licenseExpression.getChild(EXPIRY_DATE).value(), is(nullValue()));
        assertThat(licenseExpression.getChild(ISSUED_TO).value(), is(nullValue()));
        assertThat(licenseExpression.getChild(MAX_NODES).value(), is(nullValue()));
    }

    @Test
    public void testExpressionValueReturnsLicenseFields() {
        when(licenseService.currentLicense())
            .thenReturn(new LicenseData(3L, "test", 2));

        Map<String, Object> expressionValues = licenseExpression.value();
        assertThat(expressionValues.size(), is(3));
        assertThat(expressionValues.get(EXPIRY_DATE), is(3L));
        assertThat(expressionValues.get(ISSUED_TO), is("test"));
        assertThat(expressionValues.get(MAX_NODES), is(2));
    }

    @Test
    public void testGetChild() {
        when(licenseService.currentLicense())
            .thenReturn(new LicenseData(3L, "test", 2));

        assertThat(licenseExpression.getChild(EXPIRY_DATE).value(), is(3L));
        assertThat(licenseExpression.getChild(ISSUED_TO).value(), is("test"));
        assertThat(licenseExpression.getChild(MAX_NODES).value(), is(2));
    }

    @Test
    public void testGetChildAfterValueCallReturnsUpdatedLicenseFields() {
        when(licenseService.currentLicense())
            .thenReturn(new LicenseData(3L, "test", 2))
            .thenReturn(new LicenseData(4L, "test2", 3))
            .thenReturn(new LicenseData(4L, "test2", 3));

        Map<String, Object> expressionValues = licenseExpression.value();
        assertThat(expressionValues.size(), is(3));
        assertThat(expressionValues.get(EXPIRY_DATE), is(3L));
        assertThat(expressionValues.get(ISSUED_TO), is("test"));
        assertThat(expressionValues.get(MAX_NODES), is(2));

        assertThat(licenseExpression.getChild(EXPIRY_DATE).value(), is(4L));
        assertThat(licenseExpression.getChild(ISSUED_TO).value(), is("test2"));
        assertThat(licenseExpression.getChild(MAX_NODES).value(), is(3));
    }

    @Test
    public void testExpiryDateIsNullForUnlimitedSentinel() {
        when(licenseService.currentLicense())
            .thenReturn(new LicenseData(Long.MAX_VALUE, "test", 2));

        assertThat(licenseExpression.getChild(EXPIRY_DATE).value(), is(nullValue()));
    }
}
