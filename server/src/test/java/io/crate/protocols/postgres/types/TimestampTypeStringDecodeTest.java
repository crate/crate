/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.protocols.postgres.types;

import static org.assertj.core.api.Assertions.assertThat;
import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;

import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

public class TimestampTypeStringDecodeTest extends BasePGTypeTest<Long> {

    private final int numberOfFractionDigits;
    private final int timezoneDiffInHours;
    private final String era;

    public TimestampTypeStringDecodeTest(
        @Name("numberOfFractionDigits") int numberOfFractionDigits,
        @Name("era") String era,
        @Name("timezoneDiffInHours") int timezoneDiffInHours) {
        super(TimestampType.INSTANCE);
        this.numberOfFractionDigits = numberOfFractionDigits;
        this.era = era;
        this.timezoneDiffInHours = timezoneDiffInHours;
    }

    @ParametersFactory
    public static Iterable<Object[]> testParameters() {
        return Arrays.asList(
            $(0, "", 0), $(1, "", -1), $(2, "", -2), $(3, "", -3), $(4, "", -4),
            $(5, "", -5), $(6, "", 1), $(7, "", 2), $(8, "", 3), $(9, "", 4),
            $(0, " AD", 0), $(1, " AD", -1), $(2, " AD", -2), $(3, " AD", -3), $(4, " AD", -4),
            $(5, " AD", -5), $(6, " AD", 1), $(7, " AD", 2), $(8, " AD", 3), $(9, " AD", 4));
    }

    @Test
    public void testDecodeEncodeUTF8Text() {
        long expectedMillis = 1514764800000L;
        String prefix = "2018-01-01 00:00:00";

        StringBuilder fullTimestamp = new StringBuilder(prefix);
        appendFractionOfSecDigits(fullTimestamp);

        String tzString = String.format(Locale.ENGLISH, "%+03d:00", timezoneDiffInHours);
        fullTimestamp.append(tzString);
        fullTimestamp.append(era);

        if (numberOfFractionDigits > 0 && numberOfFractionDigits <= 3) {
            expectedMillis += Math.pow(10, 3 - numberOfFractionDigits);
        }

        assertThat(
            TimestampType.INSTANCE.decodeUTF8Text(fullTimestamp.toString().getBytes(StandardCharsets.UTF_8))).isEqualTo(expectedMillis);

        assertThat(
            TimestampType.INSTANCE.decodeUTF8Text(TimestampType.INSTANCE.encodeAsUTF8Text(expectedMillis)),
            is(expectedMillis)
        );
    }

    private void appendFractionOfSecDigits(StringBuilder fullTimestamp) {
        if (numberOfFractionDigits > 0) {
            fullTimestamp.append('.');
        }
        for (int i = 1; i <= numberOfFractionDigits; i++) {
            if (i == numberOfFractionDigits) {
                fullTimestamp.append('1');
            } else {
                fullTimestamp.append('0');
            }
        }
    }
}
