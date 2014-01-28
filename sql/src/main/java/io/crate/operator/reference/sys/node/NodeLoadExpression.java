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
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operator.reference.sys.node;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.sys.SysExpression;
import io.crate.metadata.sys.SystemReferences;
import io.crate.operator.reference.sys.SysObjectReference;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.monitor.os.OsService;

public class NodeLoadExpression extends SysObjectReference<Double> {

    public static final String COLNAME = "load";


    public static final String ONE = "1";
    public static final String FIVE = "5";
    public static final String FIFTEEN = "15";

    public static final ReferenceInfo INFO_LOAD = SystemReferences.registerNodeReference(
            COLNAME, DataType.OBJECT);
    public static final ReferenceInfo INFO_LOAD_1 = SystemReferences.registerNodeReference(
            COLNAME, DataType.DOUBLE, ImmutableList.of(ONE));
    public static final ReferenceInfo INFO_LOAD_5 = SystemReferences.registerNodeReference(
            COLNAME, DataType.DOUBLE, ImmutableList.of(FIVE));
    public static final ReferenceInfo INFO_LOAD_15 = SystemReferences.registerNodeReference(
            COLNAME, DataType.DOUBLE, ImmutableList.of(FIFTEEN));


    private final OsService osService;

    @Inject
    public NodeLoadExpression(OsService osService) {
        this.osService = osService;
        childImplementations.put(ONE, new LoadExpression(0, INFO_LOAD_1));
        childImplementations.put(FIVE, new LoadExpression(1, INFO_LOAD_5));
        childImplementations.put(FIFTEEN, new LoadExpression(2, INFO_LOAD_15));
    }

    class LoadExpression extends SysExpression<Double> {

        private final int idx;
        private final ReferenceInfo info;

        LoadExpression(int idx, ReferenceInfo info) {
            this.idx = idx;
            this.info = info;
        }

        @Override
        public ReferenceInfo info() {
            return info;
        }

        @Override
        public Double value() {
            return osService.stats().loadAverage()[idx];
        }
    }

    @Override
    public ReferenceInfo info() {
        return INFO_LOAD;
    }

}
