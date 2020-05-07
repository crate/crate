/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package org.elasticsearch.common.geo;

import org.apache.lucene.util.BitUtil;

import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoUtils.checkLatitude;
import static org.apache.lucene.geo.GeoUtils.checkLongitude;

/**
 * Quantizes lat/lon points and bit interleaves them into a binary morton code
 * in the range of 0x00000000... : 0xFFFFFFFF...
 * https://en.wikipedia.org/wiki/Z-order_curve
 *
 * This is useful for bitwise operations in raster space
 *
 */
public class MortonEncoder {

    private MortonEncoder() { // no instance
    }

    /**
     * Main encoding method to quantize lat/lon points and bit interleave them into a binary morton code
     * in the range of 0x00000000... : 0xFFFFFFFF...
     *
     * @param latitude latitude value: must be within standard +/-90 coordinate bounds.
     * @param longitude longitude value: must be within standard +/-180 coordinate bounds.
     * @return bit interleaved encoded values as a 64-bit {@code long}
     * @throws IllegalArgumentException if latitude or longitude is out of bounds
     */
    public static long encode(double latitude, double longitude) {
        checkLatitude(latitude);
        checkLongitude(longitude);
        // encode lat/lon flipping the sign bit so negative ints sort before positive ints
        final int latEnc = encodeLatitude(latitude) ^ 0x80000000;
        final int lonEnc = encodeLongitude(longitude) ^ 0x80000000;
        return BitUtil.interleave(lonEnc, latEnc);
    }
}
