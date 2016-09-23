/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.types;

import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class IpTypeTest extends CrateUnitTest {

    @Test
    public void testValidation() throws Exception {
        BytesRef[] validIps = {
            new BytesRef("199.199.199.200"),
            new BytesRef("192.168.0.255"),
            new BytesRef("127.0.0.1"),
            new BytesRef("123.34.243.23"),
            new BytesRef("211.121.112.111"),
            new BytesRef("0.0.0.0"),
            new BytesRef("255.255.255.255")
        };
        for (BytesRef ip : validIps) {
            assertEquals(true, IpType.isValid(ip));
            assertEquals(true, IpType.isValid(TypeTestUtils.addOffset(ip)));
        }
        BytesRef[] invalidIps = {
            new BytesRef("192.168.0.2555"),
            new BytesRef("127.0.350.1"),
            new BytesRef("127.00.350.1"),
            new BytesRef("500.34.243.23"),
            new BytesRef("211.121.1b12.111"),
            new BytesRef("00.0.0.l"),
            new BytesRef("0.255.-.255"),
            new BytesRef("0./.1.255"),
            new BytesRef("0./.1.2550"),
            new BytesRef("0.01.01.01"),
            new BytesRef("A.01.01.01"),
            new BytesRef(".192.168.0.255"),
            new BytesRef("..168.0.255"),
            new BytesRef("..168.00.255"),
            new BytesRef(".192.168.0.255."),
            new BytesRef("192.168.1.500"),
            new BytesRef("192.0000.1.1"),
            new BytesRef("192.168.1."),
            new BytesRef("192.168.."),
            new BytesRef("192.100.1"),
            new BytesRef("192.168"),
            new BytesRef("192."),
            new BytesRef(""),

        };
        for (BytesRef ip : invalidIps) {
            assertEquals(false, IpType.isValid(ip));
            assertEquals(false, IpType.isValid(TypeTestUtils.addOffset(ip)));
        }
    }

    @Test
    public void testValue() throws Exception {
        assertThat(DataTypes.IP.value(null), is(nullValue()));
        assertThat(DataTypes.IP.value("127.0.0.1"), is(new BytesRef("127.0.0.1")));

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Failed to validate ip [2000.0.0.1], not a valid ipv4 address");
        DataTypes.IP.value("2000.0.0.1");
    }
}
