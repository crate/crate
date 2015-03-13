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
import org.elasticsearch.index.mapper.ip.IpFieldMapper;

public class IpType extends StringType {

    public final static int ID = 5;
    public final static IpType INSTANCE = new IpType();
    protected IpType() {}

    @Override
    public int id() {
        return ID;
    }

    @Override
    public BytesRef value(Object value) {
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
            Long longIp = ((Number)value).longValue();
            String strIp = IpFieldMapper.longToIp(longIp);
            return new BytesRef(strIp);
        }
    }

    private void validate(BytesRef ip) {
        if(!isValid(ip))
            throw new IllegalArgumentException("Failed to validate ip ["+ ip.utf8ToString() + "], not a valid ipv4 address");
    }

    @Override
    public String getName() {
        return "ip";
    }

    @Override
    public DataType<?> create() {
        return IpType.INSTANCE;
    }

    static public boolean isValid(BytesRef ip) {
        if (ip.length < 7) { // minimum length of a valid ip address
            return false;
        }
        boolean firstSymbolInOctet = true;
        boolean firstSymbolInOctetHigherThenTwo = false;
        boolean precededByZero = false;
        short symbolsInOctet = 0;
        short numberOfDots = 0;
        for (int i = 0; i < ip.length; i++) {
            int sym = ip.bytes[i] & 0xff;
            if (sym < 46 || sym > 57 || // digits and dot symbol range
                    sym == 47) {        // a slash in a symbol range
                return false;
            }
            if (firstSymbolInOctet && (sym >= 48 && sym < 58)) {
                firstSymbolInOctet = false;
                symbolsInOctet++;
                if (sym == 48) {
                    precededByZero = true;
                }
                if (sym > 50) {
                    firstSymbolInOctetHigherThenTwo = true;
                }
            } else if ((sym == 48 && precededByZero) || symbolsInOctet > 3) {
                return false;
            } else if (sym == 46) {
                // if there are three digits in an octet and the first one is greater then '2' — it's not a valid ipv4 address
                if (symbolsInOctet > 2 && firstSymbolInOctetHigherThenTwo) {
                    return false;
                }
                firstSymbolInOctet = true;
                numberOfDots++;
                symbolsInOctet = 0;
                firstSymbolInOctetHigherThenTwo = false;
                precededByZero = false;
                if (numberOfDots > 3) {
                    return false;
                }
            } else if (sym >= 48 && sym < 58 && symbolsInOctet < 3 && !precededByZero) {
                symbolsInOctet++;
            } else {
                return false;
            }

            // if there are three digits in an octet and the first one is greater then '2' — it's not a valid ipv4 address
            if (symbolsInOctet > 2 && firstSymbolInOctetHigherThenTwo) {
                return false;
            }
        }
        return true;
    }
}
