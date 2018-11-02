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

import java.util.Locale;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public enum LicenseExpiryNotification {
    MODERATE {
        @Override
        public String notificationMessage(long millisToExpiration) {
            return String.format(Locale.ENGLISH, LICENSE_NOTIFICATION_TEMPLATE,
                MILLISECONDS.toDays(millisToExpiration), "days");
        }
    },
    SEVERE {
        @Override
        public String notificationMessage(long millisToExpiration) {
            if (millisToExpiration > 3_600_000) {
                return String.format(Locale.ENGLISH, LICENSE_NOTIFICATION_TEMPLATE,
                    MILLISECONDS.toHours(millisToExpiration), "hours");
            } else if (millisToExpiration > 60_000) {
                return String.format(Locale.ENGLISH, LICENSE_NOTIFICATION_TEMPLATE,
                    MILLISECONDS.toMinutes(millisToExpiration), "minutes");
            } else {
                return String.format(Locale.ENGLISH, LICENSE_NOTIFICATION_TEMPLATE,
                    MILLISECONDS.toSeconds(millisToExpiration), "seconds");
            }
        }
    },
    EXPIRED {
        @Override
        public String notificationMessage(long millisToExpiration) {
            return LICENSE_EXPIRED_MESSAGE;
        }
    };

    private static final String LICENSE_EXPIRED_MESSAGE = "Your CrateDB license has expired. For more information on Licensing please visit: https://crate.io/license-update/?license=expired";
    private static final String LICENSE_NOTIFICATION_TEMPLATE = "Your CrateDB license will expire in %d %s. For more information on Licensing please visit: https://crate.io/license-update";

    public abstract String notificationMessage(long millisToExpiration);
}
