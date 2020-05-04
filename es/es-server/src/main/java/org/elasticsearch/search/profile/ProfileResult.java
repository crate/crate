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

package org.elasticsearch.search.profile;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This class is the internal representation of a profiled Query, corresponding
 * to a single node in the query tree.  It is built after the query has finished executing
 * and is merely a structured representation, rather than the entity that collects the timing
 * profile (see InternalProfiler for that)
 *
 * Each InternalProfileResult has a List of InternalProfileResults, which will contain
 * "children" queries if applicable
 */
public final class ProfileResult {

    private final String type;
    private final String description;
    private final Map<String, Long> timings;
    private final long nodeTime;
    private final List<ProfileResult> children;

    public ProfileResult(String type, String description, Map<String, Long> timings, List<ProfileResult> children) {
        this.type = type;
        this.description = description;
        this.timings = Objects.requireNonNull(timings, "required timings argument missing");
        this.children = children;
        this.nodeTime = getTotalTime(timings);
    }

    /**
     * Retrieve the lucene description of this query (e.g. the "explain" text)
     */
    public String getLuceneDescription() {
        return description;
    }

    /**
     * Retrieve the name of the query (e.g. "TermQuery")
     */
    public String getQueryName() {
        return type;
    }

    /**
     * Returns the timing breakdown for this particular query node
     */
    public Map<String, Long> getTimeBreakdown() {
        return Collections.unmodifiableMap(timings);
    }

    /**
     * Returns the total time (inclusive of children) for this query node.
     *
     * @return  elapsed time in nanoseconds
     */
    public long getTime() {
        return nodeTime;
    }

    /**
     * Returns a list of all profiled children queries
     */
    public List<ProfileResult> getProfiledChildren() {
        return Collections.unmodifiableList(children);
    }

    /**
     * @param timings a map of breakdown timing for the node
     * @return The total time at this node
     */
    private static long getTotalTime(Map<String, Long> timings) {
        long nodeTime = 0;
        for (long time : timings.values()) {
            nodeTime += time;
        }
        return nodeTime;
    }

}
