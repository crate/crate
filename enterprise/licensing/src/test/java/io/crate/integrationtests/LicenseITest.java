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

import static org.hamcrest.core.Is.is;

public class LicenseITest extends SQLTransportIntegrationTest {

    public static final String ENTERPRISE_LICENSE_KEY =
        "AAAAAQAAAAIAAAEAGzjeQvokJp4ND3h8T/9/JFij7Jey2wGs4qHTR4kFXBf0dDki61pypEM7Or5p4Vb7e9t4ZErvNwwbMd9O4w" +
        "TW1ykfbJDWOul71C8pkb8VXkCNnCFHAs2Z71hY88iORn9FzB7gkJyHekv59Dpn9PH42EI52+qbi3oBprtUfbsdudx8Rwnq8ev8" +
        "4zRF57e5KVUwRvvpBlE7lqYKBLp7BrjYOWMFFKE0liNH7rUff6KBtkK0pmCYcWWp+b7TW8O3mhQoCk1GcRyyDwwhjTKuutWC+w" +
        "lBj7eoTRqinJ3aK8jr0Yi3q58Zx6Q31MYKsNjn84xwW900v3X1+XjuLJIRYJ+9uA==";

    @Test
    public void testLicenseIsAvailableInClusterStateAfterSetLicense() {
        execute("set license '" + ENTERPRISE_LICENSE_KEY + "'");

        LicenseKey licenseKey = clusterService().state().metaData().custom(LicenseKey.WRITEABLE_TYPE);
        assertThat(licenseKey, is(new LicenseKey(ENTERPRISE_LICENSE_KEY)));
    }

    @Test
    public void testLicenseCheckIsAvailableOnSysChecks() {
        SQLResponse response = execute("select severity, passed from sys.checks where id = 6");
        assertThat(response.rows()[0][0], Matchers.is(SysCheck.Severity.LOW.value()));
    }
}
