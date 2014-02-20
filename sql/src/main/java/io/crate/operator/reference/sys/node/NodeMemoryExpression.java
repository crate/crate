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

package io.crate.operator.reference.sys.node;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.ColumnIdent;
import io.crate.operator.reference.sys.SysNodeObjectReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.monitor.os.OsService;

public class NodeMemoryExpression extends SysNodeObjectReference<Object> {

    abstract class MemoryExpression extends SysNodeExpression<Object> {
        MemoryExpression(String name) {
            super(new ColumnIdent(NAME, ImmutableList.of(name)));
        }
    }

    public static final String NAME = "mem";

    public static final String FREE = "free";
    public static final String USED = "used";
    public static final String FREE_PERCENT = "free_percent";
    public static final String USED_PERCENT = "used_percent";

    private final OsService osService;

    @Inject
    public NodeMemoryExpression(OsService osService) {
        super(NAME);
        this.osService = osService;
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(FREE, new MemoryExpression(FREE) {
            @Override
            public Long value() {
                return osService.stats().mem().free().bytes();
            }
        });
        childImplementations.put(USED, new MemoryExpression(USED) {
            @Override
            public Long value() {
                return osService.stats().mem().used().bytes();
            }
        });
        childImplementations.put(FREE_PERCENT, new MemoryExpression(FREE_PERCENT) {
            @Override
            public Short value() {
                return osService.stats().mem().freePercent();
            }
        });
        childImplementations.put(USED_PERCENT, new MemoryExpression(USED_PERCENT) {
            @Override
            public Short value() {
                return osService.stats().mem().usedPercent();
            }
        });
    }

}
