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

package io.crate.types;

import io.crate.Streamer;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.InetAddresses;

import java.io.IOException;
import java.net.InetAddress;

public class IpType extends DataType<String> implements Streamer<String> {

    public static final int ID = 5;
    public static final IpType INSTANCE = new IpType();
    private static final StorageSupport<String> STORAGE = new StorageSupport<>(
        true,
        true,
        new EqQuery<String>() {

            @Override
            public Query termQuery(String field, String value) {
                return InetAddressPoint.newExactQuery(field, InetAddresses.forString(value));
            }

            @Override
            public Query rangeQuery(String field,
                                    String lowerTerm,
                                    String upperTerm,
                                    boolean includeLower,
                                    boolean includeUpper) {
                InetAddress lower;
                if (lowerTerm == null) {
                    lower = InetAddressPoint.MIN_VALUE;
                } else {
                    var lowerAddress = InetAddresses.forString(lowerTerm);
                    lower = includeLower ? lowerAddress : InetAddressPoint.nextUp(lowerAddress);
                }

                InetAddress upper;
                if (upperTerm == null) {
                    upper = InetAddressPoint.MAX_VALUE;
                } else {
                    var upperAddress = InetAddresses.forString(upperTerm);
                    upper = includeUpper ? upperAddress : InetAddressPoint.nextDown(upperAddress);
                }

                return InetAddressPoint.newRangeQuery(field, lower, upper);
            }
        }
    );

    @Override
    public int id() {
        return ID;
    }

    @Override
    public String getName() {
        return "ip";
    }

    @Override
    public Streamer<String> streamer() {
        return this;
    }

    @Override
    public String implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            validate((String) value);
            return (String) value;
        } else if (value instanceof Number) {
            long longIp = ((Number) value).longValue();
            if (longIp < 0) {
                throw new IllegalArgumentException(
                    "Failed to convert long value: " + longIp + " to ipv4 address");
            }
            return longToIp(longIp);
        } else {
            throw new ClassCastException("Can't cast '" + value + "' to " + getName());
        }
    }

    @Override
    public Precedence precedence() {
        return Precedence.IP;
    }

    @Override
    public String sanitizeValue(Object value) {
        if (value == null) {
            return null;
        } else {
            validate((String) value);
            return (String) value;
        }
    }

    private static String longToIp(long longIp) {
        int octet3 = (int) ((longIp >> 24) % 256);
        int octet2 = (int) ((longIp >> 16) % 256);
        int octet1 = (int) ((longIp >> 8) % 256);
        int octet0 = (int) ((longIp) % 256);
        return octet3 + "." + octet2 + "." + octet1 + "." + octet0;
    }

    private void validate(String ip) {
        if (!InetAddresses.isInetAddress(ip)) {
            throw new IllegalArgumentException(
                "Failed to validate ip [" + ip + "], not a valid ipv4 address");
        }
    }

    @Override
    public int compare(String val1, String val2) {
        return val1.compareTo(val2);
    }

    @Override
    public String readValueFrom(StreamInput in) throws IOException {
        return in.readOptionalString();
    }

    @Override
    public void writeValueTo(StreamOutput out, String v) throws IOException {
        out.writeOptionalString(v);
    }

    @Override
    public StorageSupport<String> storageSupport() {
        return STORAGE;
    }
}
