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

package io.crate.operation.reference.sys.matthias;

import io.crate.metadata.SimpleObjectExpression;
import io.crate.metadata.sys.SysMatthiasTableInfo;
import io.crate.operation.reference.NestedObjectExpression;

public class MatthiasStatsExpression extends NestedObjectExpression {

    public static final Long COMMITS = 890L;
    public static final Long LINES_ADDED = 164_181L;
    public static final Long LINES_REMOVED = 82_590L;
    public static final Double HEARTS_BROKEN = Double.POSITIVE_INFINITY;

    public MatthiasStatsExpression() {
        childImplementations.put(SysMatthiasTableInfo.COMMITS, new SimpleObjectExpression<Long>() {
            @Override
            public Long value() {
                return COMMITS;
            }
        });
        childImplementations.put(SysMatthiasTableInfo.LINES_ADDED, new SimpleObjectExpression<Long>() {
            @Override
            public Long value() {
                return LINES_ADDED;
            }
        });
        childImplementations.put(SysMatthiasTableInfo.LINES_REMOVED, new SimpleObjectExpression<Long>() {
            @Override
            public Long value() {
                return LINES_REMOVED;
            }
        });
        childImplementations.put(SysMatthiasTableInfo.HEARTS_BROKEN, new SimpleObjectExpression<Double>() {
            @Override
            public Double value() {
                return HEARTS_BROKEN;
            }
        });
    }
}
