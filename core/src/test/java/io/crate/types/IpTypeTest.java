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

import org.junit.Test;
import org.apache.lucene.util.BytesRef;

import static org.junit.Assert.*;

public class IpTypeTest {

    @Test
    public void testValidation() throws Exception {
        IpType ipType = new IpType();
        BytesRef[] validIps = {
            new BytesRef("192.168.0.255"),
            new BytesRef("127.0.0.1"),
            new BytesRef("123.34.243.23"),
            new BytesRef("211.121.112.111"),
            new BytesRef("0.0.0.0"),
            new BytesRef("255.255.255.255")
        };
        for (BytesRef ip : validIps) {
            assertEquals(true, ipType.isValid(ip));
        }
        BytesRef[] invalidIps = {
                new BytesRef("192.168.0.2555"),
                new BytesRef("127.0.350.1"),
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
                new BytesRef(".192.168.0.255."),

        };
        for (BytesRef ip : invalidIps) {
            assertEquals(false, ipType.isValid(ip));
        }
    }

}