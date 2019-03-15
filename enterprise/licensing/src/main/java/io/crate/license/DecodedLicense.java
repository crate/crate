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

import static io.crate.license.LicenseKey.LicenseType;

public class DecodedLicense {

    private final LicenseType type;
    private final int version;
    private final byte[] encryptedContent;

    DecodedLicense(LicenseType type, int version, byte[] encryptedContent) {
        this.type = type;
        this.version = version;
        this.encryptedContent = encryptedContent;
    }

    public LicenseType type() {
        return type;
    }

    public int version() {
        return version;
    }

    byte[] encryptedContent() {
        return encryptedContent;
    }
}
