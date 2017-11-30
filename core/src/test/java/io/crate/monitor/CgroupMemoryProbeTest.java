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

import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.io.PathUtils;
import org.junit.Assume;
import org.junit.Test;

import java.math.BigInteger;
import java.nio.file.Files;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;

public class CgroupMemoryProbeTest extends CrateUnitTest {

    @Test
    public void testReadMemoryControlGroupPath() throws Exception {
        Assume.assumeTrue(Constants.LINUX);
        Assume.assumeTrue(Files.exists(PathUtils.get("/proc/self/cgroup")));

        final String memoryControlGroupPath = CgroupMemoryProbe.getMemoryControlGroupPath();
        assertNotNull(memoryControlGroupPath);
    }

    @Test
    public void testControlGroupPathOverride() throws Exception {
        Assume.assumeTrue(Constants.LINUX);
        Assume.assumeTrue(Files.exists(PathUtils.get("/proc/self/cgroup")));
        System.setProperty("es.cgroups.hierarchy.override", "/");

        final String memoryControlGroupPath = CgroupMemoryProbe.getMemoryControlGroupPath();
        assertThat(memoryControlGroupPath, is("/"));

        System.clearProperty("es.cgroups.hierarchy.override");
    }

    @Test
    public void testCgroupMemoryProbes() throws Exception {
        Assume.assumeTrue(Constants.LINUX);
        Assume.assumeTrue(Files.exists(PathUtils.get("/sys/fs/cgroup/memory")));
        Assume.assumeTrue(Files.exists(PathUtils.get("/proc/self/cgroup")));
        ExtendedOsStats.CgroupMem cgroupMem = CgroupMemoryProbe.getCgroup();

        assertThat(new BigInteger(cgroupMem.memoryUsageBytes()), greaterThan(BigInteger.ZERO));
        assertThat(new BigInteger(cgroupMem.memoryLimitBytes()), greaterThan(BigInteger.ZERO));
    }

    @Test
    public void testNullMemoryCgroupOnNonLinuxOS() throws Exception {
        // Test only on OS != Linux
        Assume.assumeFalse(Constants.LINUX);

        ExtendedOsStats.CgroupMem cgroupMem = CgroupMemoryProbe.getCgroup();
        assertNull(cgroupMem);
    }
}
