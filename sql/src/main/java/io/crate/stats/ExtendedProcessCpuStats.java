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

package io.crate.stats;

import org.elasticsearch.common.unit.TimeValue;

public class ExtendedProcessCpuStats {

    short percent = -1;
    long sys = -1;
    long user = -1;
    long total = -1;

    public ExtendedProcessCpuStats() {
    }

    public ExtendedProcessCpuStats(short percent, long sys, long user, long total) {
        this.percent = percent;
        this.sys = sys;
        this.user = user;
        this.total = total;
    }

    /**
     * Get the Process cpu usage.
     * <p/>
     * <p>Supported Platforms: All.
     */
    public short percent() {
        return percent;
    }

    /**
     * Get the Process cpu kernel time.
     * <p/>
     * <p>Supported Platforms: All.
     */
    public TimeValue sys() {
        return new TimeValue(sys);
    }

    /**
     * Get the Process cpu user time.
     * <p/>
     * <p>Supported Platforms: All.
     */
    public TimeValue user() {
        return new TimeValue(user);
    }

    /**
     * Get the Process cpu time (sum of User and Sys).
     * <p/>
     * Supported Platforms: All.
     */
    public TimeValue total() {
        return new TimeValue(total);
    }
}
