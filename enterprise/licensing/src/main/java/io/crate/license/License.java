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

public interface License {

    enum Type {
        TRIAL(0),
        ENTERPRISE(1);

        private final int value;

        Type(int value) {
            this.value = value;
        }

        static Type of(int value) {
            switch (value) {
                case 0:
                    return TRIAL;
                case 1:
                    return ENTERPRISE;
                default:
                    throw new InvalidLicenseException("Invalid License Type of value: " + value);
            }
        }

        int value() {
            return value;
        }
    }

    Type type();

    int version();

    LicenseData licenseData();
}
