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
                new BytesRef(""),

        };
        for (BytesRef ip : invalidIps) {
            assertEquals(false, IpType.isValid(ip));
        }
    }

}