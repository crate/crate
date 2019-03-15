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

package io.crate.integrationtests;

import io.crate.expression.reference.sys.check.SysCheck;
import io.crate.license.LicenseKey;
import io.crate.testing.SQLResponse;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.elasticsearch.common.settings.Settings.builder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

public class LicenseITest extends SQLTransportIntegrationTest {

    private static final String LICENSE_KEY = "AAAAAAAAAAEAAABAFs4j1KCBd7oXN4ep073eHrdvIO8mbMadmNLFbvxK4F3kj9Cfc2HEYzmCWtLVoQ6I7ocn4g10q90QiFm/w2hm6g==";

    @Test
    public void testLicenseIsAvailableInClusterStateAfterSetLicense() {
        execute("set license '" + LICENSE_KEY + "'");

        LicenseKey licenseKey = clusterService().state().metaData().custom(LicenseKey.WRITEABLE_TYPE);
        assertThat(licenseKey, is(new LicenseKey(LICENSE_KEY)));
    }

    @Test
    public void testLicenseCheckIsAvailableOnSysChecks() {
        SQLResponse response = execute("select severity, passed from sys.checks where id = 6");
        assertThat(response.rows()[0][0], Matchers.is(SysCheck.Severity.LOW.value()));
    }
}
