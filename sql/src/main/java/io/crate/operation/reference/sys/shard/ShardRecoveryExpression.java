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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.recovery.RecoveryState;

public class ShardRecoveryExpression extends NestedObjectExpression {

    private static final String TOTAL_TIME = "total_time";
    private static final String STAGE = "stage";
    private static final String TYPE = "type";
    private static final String SIZE = "size";
    private static final String FILES = "files";


    public ShardRecoveryExpression(IndexShard indexShard) {
        addChildImplementations(indexShard.recoveryState());
    }

    private void addChildImplementations(final RecoveryState recoveryState) {
        childImplementations.put(TOTAL_TIME, new SimpleObjectExpression<Long>() {
            @Override
            public Long value() {
                return recoveryState.getTimer().time();
            }
        });
        childImplementations.put(STAGE, new SimpleObjectExpression<BytesRef>() {
            @Override
            public BytesRef value() {
                return BytesRefs.toBytesRef(recoveryState.getStage().name());
            }
        });
        childImplementations.put(TYPE, new SimpleObjectExpression<BytesRef>() {
            @Override
            public BytesRef value() {
                return BytesRefs.toBytesRef(recoveryState.getType().name());
            }
        });
        childImplementations.put(SIZE, new ShardRecoverySizeExpression(recoveryState));
        childImplementations.put(FILES, new ShardRecoveryFilesExpression(recoveryState));
    }

}
