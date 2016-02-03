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

import java.util.concurrent.TimeUnit;

public class ExtendedOsStats {

    long timestamp;
    long uptime = -1;
    double[] loadAverage = new double[0];
    Cpu cpu;

    public ExtendedOsStats(Cpu cpu) {
        this.cpu = cpu;
    }

    public long timestamp() {
        return timestamp;
    }

    public TimeValue uptime() {
        return new TimeValue(uptime, TimeUnit.SECONDS);
    }

    public double[] loadAverage() {
        return loadAverage;
    }

    public Cpu cpu() {
        return cpu;
    }

    public static class Cpu {

        short sys = -1;
        short user = -1;
        short idle = -1;
        short stolen = -1;

        public Cpu() {
        }

        public Cpu(short sys, short user, short idle, short stolen) {
            this.sys = sys;
            this.user = user;
            this.idle = idle;
            this.stolen = stolen;
        }

        public short sys() {
            return sys;
        }

        public short user() {
            return user;
        }

        public short idle() {
            return idle;
        }

        public short stolen() {
            return stolen;
        }

    }
}
