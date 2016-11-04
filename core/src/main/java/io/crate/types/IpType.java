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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.LegacyIpFieldMapper;

import java.util.Locale;

public class IpType extends StringType {

    public final static int ID = 5;
    public final static IpType INSTANCE = new IpType();

    IpType() {
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public BytesRef value(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BytesRef) {
            BytesRef ip = (BytesRef) value;
            validate(ip);
            return ip;
        }
        if (value instanceof String) {
            BytesRef ip = new BytesRef((String) value);
            validate(ip);
            return ip;
        } else {
            Long longIp = ((Number) value).longValue();
            if (longIp < 0) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Failed to convert long value: %s to ipv4 address)",
                    longIp));
            }
            String strIp = LegacyIpFieldMapper.longToIp(longIp);
            return new BytesRef(strIp);
        }
    }

    private void validate(BytesRef ip) {
        if (!isValid(ip)) {
            throw new IllegalArgumentException(
                "Failed to validate ip [" + ip.utf8ToString() + "], not a valid ipv4 address");
        }
    }

    @Override
    public String getName() {
        return "ip";
    }

    @Override
    public DataType<?> create() {
        return IpType.INSTANCE;
    }

    static boolean isValid(BytesRef ip) {
        if (ip.length < 7 || ip.length > 15) { // min/max length of a valid ip address
            return false;
        }
        boolean precededByZero = false;
        short symbolsInOctet = 0;
        short numberOfDots = 0;
        int segmentValue = 0;
        for (int i = ip.offset; i < ip.length + ip.offset; i++) {
            int sym = ip.bytes[i] & 0xff;
            if (sym < 46 || sym > 57 || sym == 47) {  // digits and dot symbol range a slash in a symbol range
                return false;
            }
            if (isDigit(sym) && symbolsInOctet < 3 && !precededByZero) {
                precededByZero = (sym == 48 && symbolsInOctet == 0);
                segmentValue = segmentValue * 10 + (sym - '0');
                symbolsInOctet++;
            } else if (sym == 46 && i < ip.length + ip.offset - 1) {
                numberOfDots++;
                if (numberOfDots > 3) {
                    return false;
                }
                segmentValue = 0;
                symbolsInOctet = 0;
                precededByZero = false;
            } else {
                return false;
            }

            if (segmentValue > 255) {
                return false;
            }
        }
        return numberOfDots == 3;
    }

    private static boolean isDigit(int sym) {
        return sym >= 48 && sym < 58;
    }
}
