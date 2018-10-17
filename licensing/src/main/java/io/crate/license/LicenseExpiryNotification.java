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
            return String.format(Locale.ENGLISH, DAYS_NOTIFICATION_TEMPLATE,
                MILLISECONDS.toDays(millisToExpiration));
        }
    },
    SEVERE {
        @Override
        public String notificationMessage(long millisToExpiration) {
            if (millisToExpiration > 3_600_000) {
                return String.format(Locale.ENGLISH, HOURS_NOTIFICATION_TEMPLATE,
                    MILLISECONDS.toHours(millisToExpiration));
            } else if (millisToExpiration > 60_000) {
                return String.format(Locale.ENGLISH, MINUTES_NOTIFICATION_TEMPLATE,
                    MILLISECONDS.toMinutes(millisToExpiration));
            } else {
                return String.format(Locale.ENGLISH, SECONDS_NOTIFICATION_LICENSE,
                    MILLISECONDS.toSeconds(millisToExpiration));
            }
        }
    },
    EXPIRED {
        @Override
        public String notificationMessage(long millisToExpiration) {
            return LICENSE_EXPIRED_MESSAGE;
        }
    };

    private static final String LICENSE_EXPIRED_MESSAGE = "Your CrateDB license is expired. Please request another license.";
    private static final String DAYS_NOTIFICATION_TEMPLATE = "Your CrateDB license will expire in %d days. Please request another license.";
    private static final String SECONDS_NOTIFICATION_LICENSE = "Your CrateDB license will expire in %d seconds. Please request another license.";
    private static final String MINUTES_NOTIFICATION_TEMPLATE = "Your CrateDB license will expire in %d minutes. Please request another license.";
    private static final String HOURS_NOTIFICATION_TEMPLATE = "Your CrateDB license will expire in %d hours. Please request another license.";

    public abstract String notificationMessage(long millisToExpiration);
}
