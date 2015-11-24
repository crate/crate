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

package io.crate.operation.reference.sys.shard;

import io.crate.metadata.SimpleObjectExpression;
import io.crate.operation.reference.NestedObjectExpression;
import org.elasticsearch.indices.recovery.RecoveryState;

public class ShardRecoverySizeExpression extends NestedObjectExpression {

    private static final String USED = "used";
    private static final String REUSED = "reused";
    private static final String RECOVERED = "recovered";
    private static final String PERCENT = "percent";

    public ShardRecoverySizeExpression(RecoveryState recoveryState) {
        addChildImplementations(recoveryState);
    }

    private void addChildImplementations(final RecoveryState recoveryState) {
        childImplementations.put(USED, new SimpleObjectExpression<Long>() {
            @Override
            public Long value() {
                return recoveryState.getIndex().totalBytes();
            }
        });
        childImplementations.put(REUSED, new SimpleObjectExpression<Long>() {
            @Override
            public Long value() {
                return recoveryState.getIndex().reusedBytes();
            }
        });
        childImplementations.put(RECOVERED, new SimpleObjectExpression<Long>() {
            @Override
            public Long value() {
                return recoveryState.getIndex().recoveredBytes();
            }
        });
        childImplementations.put(PERCENT, new SimpleObjectExpression<Float>() {
            @Override
            public Float value() {
                return recoveryState.getIndex().recoveredBytesPercent();
            }
        });
    }
}
