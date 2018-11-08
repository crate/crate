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

package io.crate.integrationtests;

import io.crate.license.LicenseKey;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class SetLicenseIntegrationTest extends SQLTransportIntegrationTest {

    private static final String LICENSE_KEY = "AAAAAAAAAAEAAABAFs4j1KCBd7oXN4ep073eHrdvIO8mbMadmNLFbvxK4F3kj9Cfc2HEYzmCWtLVoQ6I7ocn4g10q90QiFm/w2hm6g==";

    @Test
    public void testLicenseIsAvailableInClusterStateAfterSetLicense() {
        execute("set license '" + LICENSE_KEY + "'");

        LicenseKey licenseKey = clusterService().state().metaData().custom(LicenseKey.WRITEABLE_TYPE);
        assertThat(licenseKey, is(new LicenseKey(LICENSE_KEY)));
    }
}
