/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze;

import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

@Immutable
@ThreadSafe
public class PartitionedTableParameterInfo extends TableParameterInfo {

    public static final PartitionedTableParameterInfo INSTANCE = new PartitionedTableParameterInfo();

    private static final ImmutableList<String> SUPPORTED_SETTINGS =
        ImmutableList.<String>builder()
            .add(NUMBER_OF_REPLICAS)
            .add(NUMBER_OF_SHARDS)
            .add(REFRESH_INTERVAL)
            .add(READ_ONLY)
            .add(BLOCKS_READ)
            .add(BLOCKS_WRITE)
            .add(BLOCKS_METADATA)
            .add(FLUSH_THRESHOLD_SIZE)
            .add(ROUTING_ALLOCATION_ENABLE)
            .add(TOTAL_SHARDS_PER_NODE)
            .add(RECOVERY_INITIAL_SHARDS)
            .add(WARMER_ENABLED)
            .build();

    private static final TableParameterInfo PARTITION_TABLE_PARAMETER_INFO = new TablePartitionParameterInfo();

    public TableParameterInfo partitionTableSettingsInfo() {
        return PARTITION_TABLE_PARAMETER_INFO;
    }

    @Override
    public ImmutableList<String> supportedSettings() {
        return SUPPORTED_SETTINGS;
    }

    private PartitionedTableParameterInfo() {
    }
}
