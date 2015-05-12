/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
package io.crate.operation.reference.information;

import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.RoutineInfo;
import io.crate.metadata.RowCollectExpression;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.information.InformationRoutinesTableInfo;
import org.apache.lucene.util.BytesRef;

public abstract class InformationRoutinesExpression<T>
    extends RowContextCollectorExpression<RoutineInfo, T> {

    public static final RoutineNameExpression ROUTINE_NAME_EXPRESSION = new RoutineNameExpression();
    public static final RoutineTypeExpression ROUTINE_TYPE_EXPRESSION = new RoutineTypeExpression();

    protected InformationRoutinesExpression(ReferenceInfo info) {
        super(info);
    }

    public static class RoutineNameExpression extends InformationRoutinesExpression<BytesRef> {
        protected RoutineNameExpression() {
            super(InformationRoutinesTableInfo.ReferenceInfos.ROUTINE_NAME);
        }

        @Override
        public BytesRef value() {
            return new BytesRef(row.name());
        }
    }

    public static class RoutineTypeExpression extends InformationRoutinesExpression<BytesRef> {
        protected RoutineTypeExpression() {
            super(InformationRoutinesTableInfo.ReferenceInfos.ROUTINE_TYPE);
        }

        @Override
        public BytesRef value() {
            return new BytesRef(row.type());
        }
    }
}
