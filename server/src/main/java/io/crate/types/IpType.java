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

import java.io.IOException;
import java.net.InetAddress;
import java.util.function.Function;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.InetAddresses;

import io.crate.Streamer;
import io.crate.execution.dml.IpIndexer;
import io.crate.execution.dml.ValueIndexer;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

public class IpType extends DataType<String> implements Streamer<String> {

    public static final int ID = 5;
    public static final IpType INSTANCE = new IpType();
    private static final StorageSupport<String> STORAGE = new StorageSupport<>(
        true,
        false,
        true,
        new EqQuery<String>() {

            @Override
            public Query termQuery(String field, String value, boolean hasDocValues, IndexType indexType) {
                boolean isIndexed = indexType != IndexType.NONE;
                if (hasDocValues && isIndexed) {
                    return new IndexOrDocValuesQuery(
                        InetAddressPoint.newExactQuery(field, InetAddresses.forString(value)),
                        SortedSetDocValuesField.newSlowExactQuery(field, new BytesRef(InetAddressPoint.encode(InetAddresses.forString(value))))
                    );
                } else if (hasDocValues) {
                    return SortedSetDocValuesField.newSlowExactQuery(field, new BytesRef(InetAddressPoint.encode(InetAddresses.forString(value))));
                } else if (isIndexed) {
                    return InetAddressPoint.newExactQuery(field, InetAddresses.forString(value));
                }
                return null;
            }

            @Override
            public Query rangeQuery(String field,
                                    String lowerTerm,
                                    String upperTerm,
                                    boolean includeLower,
                                    boolean includeUpper,
                                    boolean hasDocValues,
                                    IndexType indexType) {
                InetAddress lower;
                if (lowerTerm == null) {
                    lower = InetAddressPoint.MIN_VALUE;
                } else {
                    var lowerAddress = InetAddresses.forString(lowerTerm);
                    lower = includeLower ? lowerAddress : InetAddressPoint.nextUp(lowerAddress);
                }
                includeLower = true; // lowerAddress has been adjusted

                InetAddress upper;
                if (upperTerm == null) {
                    upper = InetAddressPoint.MAX_VALUE;
                } else {
                    var upperAddress = InetAddresses.forString(upperTerm);
                    upper = includeUpper ? upperAddress : InetAddressPoint.nextDown(upperAddress);
                }
                includeUpper = true; // upperAddress has been adjusted

                boolean isIndexed = indexType != IndexType.NONE;
                if (hasDocValues && isIndexed) {
                    return new IndexOrDocValuesQuery(
                        InetAddressPoint.newRangeQuery(field, lower, upper),
                        SortedSetDocValuesField.newSlowRangeQuery(
                            field,
                            new BytesRef(InetAddressPoint.encode(lower)),
                            new BytesRef(InetAddressPoint.encode(upper)),
                            includeLower,
                            includeUpper));
                } else if (hasDocValues) {
                    return SortedSetDocValuesField.newSlowRangeQuery(
                        field,
                        new BytesRef(InetAddressPoint.encode(lower)),
                        new BytesRef(InetAddressPoint.encode(upper)),
                        includeLower,
                        includeUpper);
                } else if (isIndexed) {
                    return InetAddressPoint.newRangeQuery(field, lower, upper);
                }
                return null;
            }
        }
    ) {

        @Override
        public ValueIndexer<String> valueIndexer(RelationName table,
                                                 Reference ref,
                                                 Function<ColumnIdent, FieldType> getFieldType,
                                                 Function<ColumnIdent, Reference> getRef) {
            return new IpIndexer(ref, getFieldType.apply(ref.column()));
        }
    };

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
        } else if (value instanceof String str) {
            validate(str);
            return (String) value;
        } else if (value instanceof Number number) {
            long longIp = number.longValue();
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

    @Override
    public long valueBytes(String value) {
        return RamUsageEstimator.sizeOf(value);
    }
}
