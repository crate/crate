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

package org.elasticsearch.search.profile.query;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.search.profile.Timer;

/**
 * A record of timings for the various operations that may happen during query execution.
 * A node's time may be composed of several internal attributes (rewriting, weighting,
 * scoring, etc).
 */
public final class QueryProfileBreakdown {

    /**
     * The accumulated timings for this query node
     */
    private final Timer[] timings;
    private final QueryTimingType[] timingTypes;

    /** Sole constructor. */
    public QueryProfileBreakdown() {
        this.timingTypes = QueryTimingType.class.getEnumConstants();
        timings = new Timer[timingTypes.length];
        for (int i = 0; i < timings.length; ++i) {
            timings[i] = new Timer();
        }
    }

    public Timer getTimer(QueryTimingType timing) {
        return timings[timing.ordinal()];
    }

    public void setTimer(QueryTimingType timing, Timer timer) {
        timings[timing.ordinal()] = timer;
    }

    /** Convert this record to a map from timingType to times. */
    public Map<String, Long> toTimingMap() {
        HashMap<String, Long> map = HashMap.newHashMap(timingTypes.length + 2);
        for (QueryTimingType timingType : timingTypes) {
            map.put(timingType.toString(), timings[timingType.ordinal()].getApproximateTiming());
            map.put(timingType.toString() + "_count", timings[timingType.ordinal()].getCount());
        }
        return Collections.unmodifiableMap(map);
    }
}
