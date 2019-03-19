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

import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static io.crate.license.LicenseExpiryNotification.EXPIRED;
import static io.crate.license.LicenseExpiryNotification.MODERATE;
import static io.crate.license.LicenseExpiryNotification.SEVERE;
import static org.hamcrest.core.StringContains.containsString;

public class LicenseExpiryNotificationTest extends CrateUnitTest {

    @Test
    public void testModerateNotification() {
        String moderateNotification = MODERATE.message(TimeUnit.DAYS.toMillis(2));
        assertThat(moderateNotification, containsString("Your CrateDB license will expire in 2 days"));
        assertThat(moderateNotification, containsString("For more information on Licensing please visit: https://crate.io/license-update"));
    }

    @Test
    public void testSevereHoursNotification() {
        String moderateNotification = SEVERE.message(TimeUnit.HOURS.toMillis(24));
        assertThat(moderateNotification, containsString("Your CrateDB license will expire in 24 hours"));
        assertThat(moderateNotification, containsString("For more information on Licensing please visit: https://crate.io/license-update"));
    }

    @Test
    public void testSevereMinutesNotification() {
        String moderateNotification = SEVERE.message( TimeUnit.MINUTES.toMillis(46));
        assertThat(moderateNotification, containsString("Your CrateDB license will expire in 46 minutes"));
        assertThat(moderateNotification, containsString("For more information on Licensing please visit: https://crate.io/license-update"));
    }

    @Test
    public void testSevereSecondsNotification() {
        String moderateNotification = SEVERE.message( TimeUnit.SECONDS.toMillis(22));
        assertThat(moderateNotification, containsString("Your CrateDB license will expire in 22 seconds"));
        assertThat(moderateNotification, containsString("For more information on Licensing please visit: https://crate.io/license-update"));
    }

    @Test
    public void testExpiredNotification() {
        String expiredNotification = EXPIRED.message(-TimeUnit.HOURS.toMillis(4));
        assertThat(expiredNotification, containsString("Your CrateDB license has expired."));
        assertThat(expiredNotification, containsString("For more information on Licensing please visit: https://crate.io/license-update/?license=expired"));
    }
}
