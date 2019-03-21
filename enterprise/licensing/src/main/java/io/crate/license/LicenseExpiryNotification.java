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

import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public enum LicenseExpiryNotification {
    VALID {
        @Override
        public String message(long millisToExpiration) {
            return LICENSE_VALID;
        }
    },
    SEVERE {
        @Override
        public String message(long millisToExpiration) {
            if (millisToExpiration >= Duration.ofHours(1).toMillis()) {
                return String.format(Locale.ENGLISH, LICENSE_NOTIFICATION_TEMPLATE,
                    MILLISECONDS.toHours(millisToExpiration), "hours");
            } else if (millisToExpiration >= Duration.ofMinutes(1).toMillis()) {
                return String.format(Locale.ENGLISH, LICENSE_NOTIFICATION_TEMPLATE,
                    MILLISECONDS.toMinutes(millisToExpiration), "minutes");
            } else {
                return String.format(Locale.ENGLISH, LICENSE_NOTIFICATION_TEMPLATE,
                    MILLISECONDS.toSeconds(millisToExpiration), "seconds");
            }
        }
    },
    MODERATE {
        @Override
        public String message(long millisToExpiration) {
            return String.format(Locale.ENGLISH, LICENSE_NOTIFICATION_TEMPLATE,
                MILLISECONDS.toDays(millisToExpiration), "days");
        }
    },
    EXPIRED {
        @Override
        public String message(long millisToExpiration) {
            return LICENSE_EXPIRED_MESSAGE;
        }
    };

    public static LicenseExpiryNotification of(LicenseData licenseData) {
        long millisToExpiration = licenseData.millisToExpiration();
        if (millisToExpiration < 0) {
            return EXPIRED;
        } else if (millisToExpiration < TimeUnit.DAYS.toMillis(1)) {
            return SEVERE;
        } else if (millisToExpiration < TimeUnit.DAYS.toMillis(15)) {
            return MODERATE;
        }
        return VALID;
    }

    public abstract String message(long millisToExpiration);

    public static final String LICENSE_EXPIRED_MESSAGE = "Your CrateDB license has expired. For more information on Licensing please visit: https://crate.io/license-update/?license=expired";
    public static final String LICENSE_NOTIFICATION_TEMPLATE = "Your CrateDB license will expire in %d %s. For more information on Licensing please visit: https://crate.io/license-update";
    public static final String LICENSE_VALID = "Your CrateDB license is valid. Enjoy CrateDB!";
}
