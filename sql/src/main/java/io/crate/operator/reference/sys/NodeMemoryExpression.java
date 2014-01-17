/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operator.reference.sys;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.sys.SysExpression;
import io.crate.metadata.sys.SystemReferences;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.monitor.os.OsService;

public class NodeMemoryExpression extends SysObjectReference<Object> {

    abstract class MemoryExpression extends SysExpression<Object> {

        private final ReferenceInfo info;

        MemoryExpression(ReferenceInfo info) {
            this.info = info;
        }

        @Override
        public ReferenceInfo info() {
            return info;
        }

    }

    public static final String COLNAME = "mem";

    public static final String FREE = "free";
    public static final String USED = "used";
    public static final String FREE_PERCENT = "free_percent";
    public static final String USED_PERCENT = "used_percent";

    public static final ReferenceInfo INFO_MEM = SystemReferences.registerNodeReference(
            COLNAME, DataType.OBJECT);
    public static final ReferenceInfo INFO_MEM_FREE = SystemReferences.registerNodeReference(
            COLNAME, DataType.LONG, ImmutableList.of(FREE));
    public static final ReferenceInfo INFO_MEM_USED = SystemReferences.registerNodeReference(
            COLNAME, DataType.LONG, ImmutableList.of(USED));
    public static final ReferenceInfo INFO_MEM_FREE_PERCENT = SystemReferences.registerNodeReference(
            COLNAME, DataType.SHORT, ImmutableList.of(FREE_PERCENT));
    public static final ReferenceInfo INFO_MEM_USED_PERCENT = SystemReferences.registerNodeReference(
            COLNAME, DataType.SHORT, ImmutableList.of(USED_PERCENT));


    private final OsService osService;

    @Inject
    public NodeMemoryExpression(OsService osService) {
        this.osService = osService;
        addChildImplementations();
    }

    @Override
    public ReferenceInfo info() {
        return INFO_MEM;
    }

    private void addChildImplementations() {
        childImplementations.put(FREE, new MemoryExpression(INFO_MEM_FREE) {
            @Override
            public Long value() {
                return osService.stats().mem().free().bytes();
            }
        });
        childImplementations.put(USED, new MemoryExpression(INFO_MEM_USED) {
            @Override
            public Long value() {
                return osService.stats().mem().used().bytes();
            }
        });
        childImplementations.put(FREE_PERCENT, new MemoryExpression(INFO_MEM_FREE_PERCENT) {
            @Override
            public Short value() {
                return osService.stats().mem().freePercent();
            }
        });
        childImplementations.put(USED_PERCENT, new MemoryExpression(INFO_MEM_USED_PERCENT) {
            @Override
            public Short value() {
                return osService.stats().mem().usedPercent();
            }
        });
    }

}
