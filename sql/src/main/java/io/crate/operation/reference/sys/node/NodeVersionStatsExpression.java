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

package io.crate.operation.reference.sys.node;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

public class NodeVersionStatsExpression extends NestedNodeStatsExpression {

    private static final String NUMBER = "number";
    private static final String BUILD_HASH = "build_hash";
    private static final String BUILD_SNAPSHOT = "build_snapshot";

    public NodeVersionStatsExpression() {
        childImplementations.put(NUMBER, new SimpleNodeStatsExpression<BytesRef>() {
            @Override
            public BytesRef innerValue() {
                return BytesRefs.toBytesRef(this.row.version().number());
            }
        });
        childImplementations.put(BUILD_HASH, new SimpleNodeStatsExpression<BytesRef>() {
            @Override
            public BytesRef innerValue() {
                return BytesRefs.toBytesRef(this.row.build().hash());
            }
        });
        childImplementations.put(BUILD_SNAPSHOT, new SimpleNodeStatsExpression<Boolean>() {
            @Override
            public Boolean innerValue() {
                return this.row.version().snapshot();
            }
        });
    }
}
