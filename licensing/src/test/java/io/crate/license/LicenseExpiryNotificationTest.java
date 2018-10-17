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

import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static io.crate.license.LicenseExpiryNotification.EXPIRED;
import static io.crate.license.LicenseExpiryNotification.MODERATE;
import static io.crate.license.LicenseExpiryNotification.SEVERE;
import static org.hamcrest.core.StringContains.containsString;

public class LicenseExpiryNotificationTest extends CrateUnitTest {

    @Test
    public void testModerateNotificatiton() {
        String moderateNotification = MODERATE.notificationMessage(TimeUnit.DAYS.toMillis(2));
        assertThat(moderateNotification, containsString("Your CrateDB license will expire in 2 days"));
        assertThat(moderateNotification, containsString("Please request another license."));
    }

    @Test
    public void testSevereHoursNotificaiton() {
        String moderateNotification = SEVERE.notificationMessage(TimeUnit.HOURS.toMillis(24));
        assertThat(moderateNotification, containsString("Your CrateDB license will expire in 24 hours"));
        assertThat(moderateNotification, containsString("Please request another license."));
    }

    @Test
    public void testSevereMinutesNotification() {
        String moderateNotification = SEVERE.notificationMessage( TimeUnit.MINUTES.toMillis(46));
        assertThat(moderateNotification, containsString("Your CrateDB license will expire in 46 minutes"));
        assertThat(moderateNotification, containsString("Please request another license."));
    }

    @Test
    public void testSevereSecondsNotification() {
        String moderateNotification = SEVERE.notificationMessage( TimeUnit.SECONDS.toMillis(22));
        assertThat(moderateNotification, containsString("Your CrateDB license will expire in 22 seconds"));
        assertThat(moderateNotification, containsString("Please request another license."));
    }

    @Test
    public void testExpiredNotification() {
        String expiredNotification = EXPIRED.notificationMessage(-TimeUnit.HOURS.toMillis(4));
        assertThat(expiredNotification, containsString("Your CrateDB license is expired."));
        assertThat(expiredNotification, containsString("Please request another license."));
    }
}
