/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.elasticsearch.monitor.os;

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class OsProbeTests extends ESTestCase {

    @Test
    public void test_memory_limit_never_exceeds_total_physical_memory() {
        OsProbe osProbe = OsProbe.getInstance();
        long totalPhysical = osProbe.getTotalPhysicalMemorySize();
        long memoryLimit = osProbe.getMemoryLimit();

        assertThat(memoryLimit).isGreaterThan(0L);
        assertThat(memoryLimit).isLessThanOrEqualTo(totalPhysical);
    }

    @Test
    public void test_parse_cgroup_memory_limit_in_bytes_treats_a_missing_value_as_no_limit() {
        assertThat(OsProbe.parseCgroupMemoryLimitInBytes(null)).isEqualTo(-1L);
    }

    @Test
    public void test_parse_cgroup_memory_limit_in_bytes_treats_cgroups_v2s_max_as_no_limit() {
        assertThat(OsProbe.parseCgroupMemoryLimitInBytes("max")).isEqualTo(-1L);
    }

    @Test
    public void test_parse_cgroup_memory_limit_in_bytes_parses_a_real_byte_count() {
        assertThat(OsProbe.parseCgroupMemoryLimitInBytes("1073741824")).isEqualTo(1073741824L);
    }

    @Test
    public void test_parse_cgroup_memory_limit_in_bytes_parses_cgroups_v1s_unconfined_sentinel() {
        // cgroups v1's sentinel for "unconfined" - must still parse as a real number here;
        // getMemoryLimit() is what treats it as "no limit", not this parsing step.
        assertThat(OsProbe.parseCgroupMemoryLimitInBytes("9223372036854771712")).isEqualTo(9223372036854771712L);
    }

    @Test
    public void test_parse_cgroup_memory_limit_in_bytes_treats_unparsable_text_as_no_limit() {
        assertThat(OsProbe.parseCgroupMemoryLimitInBytes("not-a-number")).isEqualTo(-1L);
    }
}
