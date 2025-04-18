/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.unit;

import java.util.Locale;
import java.util.Objects;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;

import ch.randelshofer.fastdoubleparser.JavaDoubleParser;

public final class ByteSizeValue implements Comparable<ByteSizeValue> {

    public static final ByteSizeValue ZERO = new ByteSizeValue(0, ByteSizeUnit.BYTES);

    private final long size;
    private final ByteSizeUnit unit;

    public ByteSizeValue(long bytes) {
        this(bytes, ByteSizeUnit.BYTES);
    }

    public ByteSizeValue(long size, ByteSizeUnit unit) {
        if (size < -1 || (size == -1 && unit != ByteSizeUnit.BYTES)) {
            throw new IllegalArgumentException("Values less than -1 bytes are not supported: " + size + unit.getSuffix());
        }
        if (size > Long.MAX_VALUE / unit.toBytes(1)) {
            throw new IllegalArgumentException(
                    "Values greater than " + Long.MAX_VALUE + " bytes are not supported: " + size + unit.getSuffix());
        }
        this.size = size;
        this.unit = unit;
    }

    /**
     * Returns the size as int.
     * You should use {@link #getBytes()} instead if possible.
     *
     * @throws IllegalArgumentException if the value exceeds {@link Integer#MAX_VALUE}
     */
    public int bytesAsInt() {
        long bytes = getBytes();
        if (bytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("size [" + toString() + "] is bigger than max int");
        }
        return (int) bytes;
    }

    public long getBytes() {
        return unit.toBytes(size);
    }

    public double getMbFrac() {
        return ((double) getBytes()) / ByteSizeUnit.C2;
    }

    /**
     * @return a string representation of this value which is guaranteed to be
     *         able to be parsed using
     *         {@link #parseBytesSizeValue(String, ByteSizeValue, String)}.
     *         Unlike {@link #toString()} this method will not output fractional
     *         or rounded values so this method should be preferred when
     *         serialising the value to JSON.
     */
    public String getStringRep() {
        if (size <= 0) {
            return String.valueOf(size);
        }
        return size + unit.getSuffix();
    }

    @Override
    public String toString() {
        return humanReadableBytes(getBytes());
    }

    public static String humanReadableBytes(long bytes) {
        String prefix = "";
        if (bytes < 0) {
            bytes = bytes * -1;
            prefix = "-";
        }
        double value = bytes;
        String suffix = ByteSizeUnit.BYTES.getSuffix();
        if (bytes >= ByteSizeUnit.C5) {
            value = ((double) bytes) / ByteSizeUnit.C5;
            suffix = ByteSizeUnit.PB.getSuffix();
        } else if (bytes >= ByteSizeUnit.C4) {
            value = ((double) bytes) / ByteSizeUnit.C4;
            suffix = ByteSizeUnit.TB.getSuffix();
        } else if (bytes >= ByteSizeUnit.C3) {
            value = ((double) bytes) / ByteSizeUnit.C3;
            suffix = ByteSizeUnit.GB.getSuffix();
        } else if (bytes >= ByteSizeUnit.C2) {
            value = ((double) bytes) / ByteSizeUnit.C2;
            suffix = ByteSizeUnit.MB.getSuffix();
        } else if (bytes >= ByteSizeUnit.C1) {
            value = ((double) bytes) / ByteSizeUnit.C1;
            suffix = ByteSizeUnit.KB.getSuffix();
        }
        return prefix + Strings.format1Decimals(value, suffix);
    }

    public static ByteSizeValue parseBytesSizeValue(String sValue, String settingName) throws ElasticsearchParseException {
        return parseBytesSizeValue(sValue, null, settingName);
    }

    public static ByteSizeValue parseBytesSizeValue(String sValue, ByteSizeValue defaultValue, String settingName)
            throws ElasticsearchParseException {
        settingName = Objects.requireNonNull(settingName);
        if (sValue == null) {
            return defaultValue;
        }
        String lowerSValue = sValue.toLowerCase(Locale.ROOT).trim();
        if (lowerSValue.endsWith("k")) {
            return parse(sValue, lowerSValue, "k", ByteSizeUnit.KB, settingName);
        } else if (lowerSValue.endsWith("kb")) {
            return parse(sValue, lowerSValue, "kb", ByteSizeUnit.KB, settingName);
        } else if (lowerSValue.endsWith("m")) {
            return parse(sValue, lowerSValue, "m", ByteSizeUnit.MB, settingName);
        } else if (lowerSValue.endsWith("mb")) {
            return parse(sValue, lowerSValue, "mb", ByteSizeUnit.MB, settingName);
        } else if (lowerSValue.endsWith("g")) {
            return parse(sValue, lowerSValue, "g", ByteSizeUnit.GB, settingName);
        } else if (lowerSValue.endsWith("gb")) {
            return parse(sValue, lowerSValue, "gb", ByteSizeUnit.GB, settingName);
        } else if (lowerSValue.endsWith("t")) {
            return parse(sValue, lowerSValue, "t", ByteSizeUnit.TB, settingName);
        } else if (lowerSValue.endsWith("tb")) {
            return parse(sValue, lowerSValue, "tb", ByteSizeUnit.TB, settingName);
        } else if (lowerSValue.endsWith("p")) {
            return parse(sValue, lowerSValue, "p", ByteSizeUnit.PB, settingName);
        } else if (lowerSValue.endsWith("pb")) {
            return parse(sValue, lowerSValue, "pb", ByteSizeUnit.PB, settingName);
        } else if (lowerSValue.endsWith("b")) {
            return new ByteSizeValue(Long.parseLong(lowerSValue.substring(0, lowerSValue.length() - 1).trim()), ByteSizeUnit.BYTES);
        } else if (lowerSValue.equals("-1")) {
            // Allow this special value to be unit-less:
            return new ByteSizeValue(-1, ByteSizeUnit.BYTES);
        } else if (lowerSValue.equals("0")) {
            // Allow this special value to be unit-less:
            return new ByteSizeValue(0, ByteSizeUnit.BYTES);
        } else {
            // Missing units == expect bytes
            return parse(sValue, lowerSValue, "", ByteSizeUnit.BYTES, settingName);
        }
    }

    private static ByteSizeValue parse(final String initialInput, final String normalized, final String suffix, ByteSizeUnit unit,
            final String settingName) {
        final String s = normalized.substring(0, normalized.length() - suffix.length()).trim();
        try {
            try {
                return new ByteSizeValue(Long.parseLong(s), unit);
            } catch (final NumberFormatException e) {
                try {
                    final double doubleValue = JavaDoubleParser.parseDouble(s);
                    return new ByteSizeValue((long) (doubleValue * unit.toBytes(1)));
                } catch (final NumberFormatException ignored) {
                    throw new ElasticsearchParseException("failed to parse setting [{}] with value [{}] as a size in bytes", e, settingName,
                        initialInput);
                }
            }
        } catch (IllegalArgumentException e) {
            throw new ElasticsearchParseException("failed to parse setting [{}] with value [{}] as a size in bytes", e, settingName,
                    initialInput);
        }
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ByteSizeValue other && compareTo(other) == 0;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(size * unit.toBytes(1));
    }

    @Override
    public int compareTo(ByteSizeValue other) {
        long thisValue = size * unit.toBytes(1);
        long otherValue = other.size * other.unit.toBytes(1);
        return Long.compare(thisValue, otherValue);
    }
}
