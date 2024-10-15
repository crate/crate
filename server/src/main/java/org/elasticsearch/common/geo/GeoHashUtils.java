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

package org.elasticsearch.common.geo;

import org.apache.lucene.util.BitUtil;

/**
 * Utilities for converting to/from the GeoHash standard
 *
 * The geohash long format is represented as lon/lat (x/y) interleaved with the 4 least significant bits
 * representing the level (1-12) [xyxy...xyxyllll]
 *
 * This differs from a morton encoded value which interleaves lat/lon (y/x).*
 */
public class GeoHashUtils {
    private static final char[] BASE_32 = {'0', '1', '2', '3', '4', '5', '6',
        '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n',
        'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

    /** maximum precision for geohash strings */
    public static final int PRECISION = 12;
    /** number of bits used for quantizing latitude and longitude values */
    public static final short BITS = 31;
    /** scaling factors to convert lat/lon into unsigned space */
    private static final short MORTON_OFFSET = (BITS << 1) - (PRECISION * 5);

    // No instance:
    private GeoHashUtils() {
    }

    /*************************
     * 31 bit encoding utils *
     *************************/
    public static long encodeLatLon(final double lat, final double lon) {
        return MortonEncoder.encode(lat, lon) >>> 2;
    }

    /**
     * Convert from a morton encoded long from a geohash encoded long
     */
    public static long fromMorton(long morton, int level) {
        long mFlipped = BitUtil.flipFlop(morton);
        mFlipped >>>= (((GeoHashUtils.PRECISION - level) * 5) + MORTON_OFFSET);
        return (mFlipped << 4) | level;
    }

    /**
     * Encode to a geohash string from full resolution longitude, latitude)
     */
    public static final String stringEncode(final double lon, final double lat) {
        int level = 12;
        long ghLong = fromMorton(encodeLatLon(lat, lon), level);
        int level1 = (int) ghLong & 15;
        ghLong >>>= 4;
        char[] chars = new char[level1];
        do {
            chars[--level1] = BASE_32[(int) (ghLong & 31L)];
            ghLong >>>= 5;
        } while (level1 > 0);
        return new String(chars);
    }
}
