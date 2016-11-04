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

package io.crate.monitor;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

public class ExtendedProcessCpuStats implements Streamable {

    private short percent;
    private TimeValue sys;
    private TimeValue user;
    private TimeValue total;

    public static ExtendedProcessCpuStats readExtendedProcessCpuStats(StreamInput in) throws IOException {
        ExtendedProcessCpuStats stat = new ExtendedProcessCpuStats();
        stat.readFrom(in);
        return stat;
    }

    public ExtendedProcessCpuStats() {
        this((short) -1, -1L, -1L, -1L);
    }

    public ExtendedProcessCpuStats(short percent, long sys, long user, long total) {
        this.percent = percent;
        this.sys = new TimeValue(sys);
        this.user = new TimeValue(user);
        this.total = new TimeValue(total);
    }

    /**
     * Get the Process cpu usage.
     * <p/>
     * <p>Supported Platforms: All.
     */
    public short percent() {
        return percent;
    }

    public void percent(short percent) {
        this.percent = percent;
    }

    /**
     * Get the Process cpu kernel time.
     * <p/>
     * <p>Supported Platforms: All.
     */
    public TimeValue sys() {
        return sys;
    }

    public void sys(TimeValue sys) {
        this.sys = sys;
    }

    /**
     * Get the Process cpu user time.
     * <p/>
     * <p>Supported Platforms: All.
     */
    public TimeValue user() {
        return user;
    }

    public void user(TimeValue user) {
        this.user = user;
    }

    /**
     * Get the Process cpu time (sum of User and Sys).
     * <p/>
     * Supported Platforms: All.
     */
    public TimeValue total() {
        return total;
    }

    public void total(TimeValue total) {
        this.total = total;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        percent = in.readShort();
        sys = new TimeValue(in);
        user = new TimeValue(in);
        total = new TimeValue(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeShort(percent);
        sys.writeTo(out);
        user.writeTo(out);
        total.writeTo(out);
    }
}
