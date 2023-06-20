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

package io.crate.memory;

import io.crate.data.breaker.RamAccounting;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

import java.util.Set;
import java.util.function.Function;

@Singleton
public final class MemoryManagerFactory implements Function<RamAccounting, MemoryManager> {


    enum MemoryType {
        ON_HEAP("on-heap"),
        OFF_HEAP("off-heap");

        private final String value;

        MemoryType(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }

        public static MemoryType of(String value) {
            if (value.equals(ON_HEAP.value)) {
                return ON_HEAP;
            } else if (value.equals(OFF_HEAP.value)) {
                return OFF_HEAP;
            }
            throw new IllegalArgumentException("Invalid memory type `" + value + "`. Expected one of: " + TYPES);
        }
    }

    private static final Set<String> TYPES = Set.of(MemoryType.OFF_HEAP.value(), MemoryType.ON_HEAP.value());

    // Explicit generic is required for eclipse JDT, otherwise it won't compile
    public static final Setting<String> MEMORY_ALLOCATION_TYPE = new Setting<String>(
        "memory.allocation.type",
        MemoryType.ON_HEAP.value(),
        input -> {
            if (TYPES.contains(input)) {
                return input;
            }
            throw new IllegalArgumentException(
                "Invalid argument `" + input + "` for `memory.allocation.type`, valid values are: " + TYPES);
        },
        DataTypes.STRING,
        Property.NodeScope,
        Property.Dynamic,
        Property.Exposed
    );

    private volatile MemoryType currentMemoryType = MemoryType.ON_HEAP;

    @Inject
    public MemoryManagerFactory(ClusterSettings clusterSettings) {
        clusterSettings.addSettingsUpdateConsumer(MEMORY_ALLOCATION_TYPE, newValue -> {
            currentMemoryType = MemoryType.of(newValue);
        });
    }

    /**
     * @return a MemoryManager instance that doesn't support concurrent access.
     *         Any component acquiring a MemoryManager must make sure to close it after use.
     */
    public MemoryManager getMemoryManager(RamAccounting ramAccounting) {
        switch (currentMemoryType) {
            case ON_HEAP:
                return new OnHeapMemoryManager(ramAccounting::addBytes);
            case OFF_HEAP:
                return new OffHeapMemoryManager();
            default:
                throw new AssertionError("MemoryType is supposed to have only 2 cases");
        }
    }

    @Override
    public MemoryManager apply(RamAccounting ramAccounting) {
        return getMemoryManager(ramAccounting);
    }
}
