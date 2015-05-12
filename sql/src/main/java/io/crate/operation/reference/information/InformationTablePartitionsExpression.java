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
package io.crate.operation.reference.information;

import com.google.common.base.MoreObjects;
import io.crate.metadata.*;
import io.crate.metadata.information.InformationPartitionsTableInfo;
import org.apache.lucene.util.BytesRef;

import java.util.Map;

public abstract class InformationTablePartitionsExpression<T>
        extends RowContextCollectorExpression<PartitionInfo, T> {

    public static final PartitionsTableNameExpression TABLE_NAME_EXPRESSION = new PartitionsTableNameExpression();
    public static final PartitionsSchemaNameExpression SCHEMA_NAME_EXPRESSION = new PartitionsSchemaNameExpression();
    public static final PartitionsPartitionIdentExpression PARTITION_IDENT_EXPRESSION = new PartitionsPartitionIdentExpression();
    public static final PartitionsValuesExpression VALUES_EXPRESSION = new PartitionsValuesExpression();
    public static final PartitionsNumberOfShardsExpression NUMBER_OF_SHARDS_EXPRESSION = new PartitionsNumberOfShardsExpression();
    public static final PartitionsNumberOfReplicasExpression NUMBER_OF_REPLICAS_EXPRESSION = new PartitionsNumberOfReplicasExpression();

    protected InformationTablePartitionsExpression(ReferenceInfo info) {
        super(info);
    }

    public static class PartitionsTableNameExpression extends InformationTablePartitionsExpression<BytesRef> {
        protected PartitionsTableNameExpression() {
            super(InformationPartitionsTableInfo.ReferenceInfos.TABLE_NAME);
        }

        @Override
        public BytesRef value() {
            return new BytesRef(row.name().tableName());
        }
    }

    public static class PartitionsSchemaNameExpression extends InformationTablePartitionsExpression<BytesRef> {
        protected PartitionsSchemaNameExpression() {
            super(InformationPartitionsTableInfo.ReferenceInfos.SCHEMA_NAME);
        }

        @Override
        public BytesRef value() {
            return new BytesRef(
                    MoreObjects.firstNonNull(row.name().schemaName(), ReferenceInfos.DEFAULT_SCHEMA_NAME)
            );
        }
    }
    public static class PartitionsPartitionIdentExpression extends InformationTablePartitionsExpression<BytesRef> {
        protected PartitionsPartitionIdentExpression() {
            super(InformationPartitionsTableInfo.ReferenceInfos.PARTITION_IDENT);
        }

        @Override
        public BytesRef value() {
            return new BytesRef(row.name().ident());
        }
    }
    public static class PartitionsValuesExpression extends InformationTablePartitionsExpression<Map<String, Object>> {
        protected PartitionsValuesExpression() {
            super(InformationPartitionsTableInfo.ReferenceInfos.VALUES);
        }

        @Override
        public Map<String, Object> value() {
            return row.values();
        }
    }
    public static class PartitionsNumberOfShardsExpression extends InformationTablePartitionsExpression<Integer> {

        protected PartitionsNumberOfShardsExpression() {
            super(InformationPartitionsTableInfo.ReferenceInfos.NUMBER_OF_SHARDS);
        }

        @Override
        public Integer value() {
            return row.numberOfShards();
        }
    }

    public static class PartitionsNumberOfReplicasExpression extends InformationTablePartitionsExpression<BytesRef> {

        protected PartitionsNumberOfReplicasExpression() {
            super(InformationPartitionsTableInfo.ReferenceInfos.NUMBER_OF_REPLICAS);
        }

        @Override
        public BytesRef value() {
            return row.numberOfReplicas();
        }
    }
}
