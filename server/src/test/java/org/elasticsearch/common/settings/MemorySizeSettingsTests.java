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

package org.elasticsearch.common.settings;

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class MemorySizeSettingsTests extends ESTestCase {

    @Test
    public void testPageCacheLimitHeapSetting() {
        assertMemorySizeSetting(PageCacheRecycler.LIMIT_HEAP_SETTING, "cache.recycler.page.limit.heap",
                new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.1)));
    }

    @Test
    public void testIndexBufferSizeSetting() {
        assertMemorySizeSetting(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "indices.memory.index_buffer_size",
                new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.1)));
    }

    @Test
    public void testQueryCacheSizeSetting() {
        assertMemorySizeSetting(IndicesQueryCache.INDICES_CACHE_QUERY_SIZE_SETTING, "indices.queries.cache.size",
                new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.1)));
    }

    @Test
    public void testCircuitBreakerSettings() {
        double defaultTotalPercentage = 0.95d;
        assertMemorySizeSetting(HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING, "indices.breaker.total.limit",
                new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * defaultTotalPercentage)));
        assertMemorySizeSetting(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING, "indices.breaker.request.limit",
                new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.6)));
        assertMemorySizeSetting(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING,
                "network.breaker.inflight_requests.limit", new ByteSizeValue((JvmInfo.jvmInfo().getMem().getHeapMax().getBytes())));
    }


    private void assertMemorySizeSetting(Setting<ByteSizeValue> setting, String settingKey, ByteSizeValue defaultValue) {
        assertThat(setting).isNotNull();
        assertThat(setting.getKey()).isEqualTo(settingKey);
        assertThat(setting.getProperties()).contains(Property.NodeScope);
        assertThat(setting.getDefault(Settings.EMPTY)).isEqualTo(defaultValue);
        Settings settingWithPercentage = Settings.builder().put(settingKey, "25%").build();
        assertThat(setting.get(settingWithPercentage)).isEqualTo(new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.25)));
        Settings settingWithBytesValue = Settings.builder().put(settingKey, "1024b").build();
        assertThat(setting.get(settingWithBytesValue)).isEqualTo(new ByteSizeValue(1024));
    }
}
