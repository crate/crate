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

import com.google.common.annotations.VisibleForTesting;
import org.elasticsearch.common.io.PathUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * This Class is a subset of the OsStats Service implementation of of ES 6
 * It provides methods to fetch cgroup memory metrics from the filesystem located in <code>/proc/self/cgroup</code>.
 *
 * TODO: The implementation gets obsolete and can be removed when upgrading to ES 6
 *
 * @see <a href="github.com/elastic/elasticsearch/blob/6.1/core/src/main/java/org/elasticsearch/monitor/os/OsProbe.java">OsProbe</a>
 *
 */
public class CgroupMemoryProbe {

    private static String readSingleLine(final Path path) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line = reader.readLine();
            assert line != null : "cgroup path leads to an empty file";
            return line;
        }
    }

    /**
     * The maximum amount of user memory (including file cache).
     * It is possible that the result value overflows a <code>Long</code> therefore the result type is <code>String</code>
     */
    private static String getCgroupMemoryLimitInBytes(final String controlGroup) throws IOException {
        return readSingleLine(PathUtils.get("/sys/fs/cgroup/memory", controlGroup, "memory.limit_in_bytes"));
    }

    /**
     * The total current memory usage by processes in the cgroup (in bytes).
     * It is possible that the result value overflows a <code>Long</code> therefore the result type is <code>String</code>
     */
    private static String getCgroupMemoryUsageInBytes(final String controlGroup) throws IOException {
        return readSingleLine(PathUtils.get("/sys/fs/cgroup/memory", controlGroup, "memory.usage_in_bytes"));
    }

    private static boolean areCgroupStatsAvailable() {
        if (!Files.exists(PathUtils.get("/proc/self/cgroup"))) {
            return false;
        }
        if (!Files.exists(PathUtils.get("/sys/fs/cgroup/memory"))) {
            return false;
        }
        return true;
    }

    /**
     * Returns the cgroup path to which the CrateDB process belongs.
     */
    @VisibleForTesting
    static String getMemoryControlGroupPath() throws IOException {
        // this property is to support a hack to workaround an issue with Docker containers mounting the cgroups hierarchy
        // inconsistently with respect to /proc/self/cgroup; for Docker containers this should be set to "/"
        final String CONTROL_GROUPS_HIERARCHY_OVERRIDE = System.getProperty("es.cgroups.hierarchy.override");
        final String CGROUP_MEMORY = "memory";

        try (BufferedReader reader = Files.newBufferedReader(PathUtils.get("/proc/self/cgroup"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Each line contains three colon-separated fields of the form hierarchy-ID:subsystem-list:cgroup-path.
                final String[] fields = line.split(":");
                assert fields.length == 3 : "cgroup file has an incorrect hierarchy pattern";
                final String[] controllers = fields[1].split(",");
                for (final String controller : controllers) {
                    final String controlGroupPath;
                    if (CONTROL_GROUPS_HIERARCHY_OVERRIDE != null) {
                        // Docker violates the relationship between /proc/self/cgroup and the /sys/fs/cgroup hierarchy.
                        // It's possible that this will be fixed in future versions of Docker with cgroup namespaces, but
                        // this requires modern kernels.
                        controlGroupPath = CONTROL_GROUPS_HIERARCHY_OVERRIDE;
                    } else {
                        controlGroupPath = fields[2];
                    }
                    if (controller.equals(CGROUP_MEMORY)) {
                        return controlGroupPath;
                    }
                }
            }
            return null;
        }
    }

    static ExtendedOsStats.CgroupMem getCgroup() {
        try {
            if (!areCgroupStatsAvailable()) {
                return null;
            } else {

                final String memoryControlGroup = getMemoryControlGroupPath();
                assert memoryControlGroup != null : "memory cgroup path must not be null";
                assert memoryControlGroup.isEmpty() == false : "memory cgroup path must no be empty";

                final String cgroupMemoryLimitInBytes = getCgroupMemoryLimitInBytes(memoryControlGroup);
                final String cgroupMemoryUsageInBytes = getCgroupMemoryUsageInBytes(memoryControlGroup);

                return new ExtendedOsStats.CgroupMem(
                    memoryControlGroup,
                    cgroupMemoryLimitInBytes,
                    cgroupMemoryUsageInBytes);
            }
        } catch (IOException e) {
            return null;
        }
    }
}
