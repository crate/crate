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

import io.crate.metadata.PartitionInfo;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.doc.DocIndexMetaData;
import org.apache.lucene.util.BytesRef;

import java.util.Map;

abstract class InformationTablePartitionsExpression<T>
    extends RowContextCollectorExpression<PartitionInfo, T> {

    public static final BytesRef TABLE_TYPE = new BytesRef("BASE TABLE");
    public static final BytesRef SELF_REFERENCING_COLUMN_NAME = new BytesRef("_id");
    public static final BytesRef REFERENCE_GENERATION = new BytesRef("SYSTEM GENERATED");

    public static class PartitionsTableNameExpression extends InformationTablePartitionsExpression<BytesRef> {
        @Override
        public BytesRef value() {
            return new BytesRef(row.name().tableIdent().name());
        }
    }

    public static class PartitionsSchemaNameExpression extends InformationTablePartitionsExpression<BytesRef> {

        @Override
        public BytesRef value() {
            return new BytesRef(row.name().tableIdent().schema());
        }
    }

    public static class PartitionsTableCatalogExpression extends InformationTablePartitionsExpression<BytesRef> {

        @Override
        public BytesRef value() {
            return new BytesRef(row.name().tableIdent().schema());
        }
    }

    public static class PartitionsTableTypeExpression extends InformationTablePartitionsExpression<BytesRef> {

        @Override
        public BytesRef value() {
            return TABLE_TYPE;
        }
    }

    public static class PartitionsPartitionIdentExpression extends InformationTablePartitionsExpression<BytesRef> {
        @Override
        public BytesRef value() {
            return new BytesRef(row.name().ident());
        }
    }

    public static class PartitionsValuesExpression extends InformationTablePartitionsExpression<Map<String, Object>> {
        @Override
        public Map<String, Object> value() {
            return row.values();
        }
    }

    public static class PartitionsNumberOfShardsExpression extends InformationTablePartitionsExpression<Integer> {

        @Override
        public Integer value() {
            return row.numberOfShards();
        }
    }

    public static class PartitionsNumberOfReplicasExpression extends InformationTablePartitionsExpression<BytesRef> {
        @Override
        public BytesRef value() {
            return row.numberOfReplicas();
        }
    }

    public static class PartitionsRoutingHashFunctionExpression
        extends InformationTablePartitionsExpression<BytesRef> {
        @Override
        public BytesRef value() {
            return new BytesRef(DocIndexMetaData.getRoutingHashFunctionPrettyName(row.routingHashFunction()));
        }
    }

    public static class ClosedExpression extends InformationTablePartitionsExpression<Boolean> {
        @Override
        public Boolean value() {
            return row.isClosed();
        }
    }

    public static class PartitionsSelfReferencingColumnNameExpression extends InformationTablePartitionsExpression<BytesRef> {
        @Override
        public BytesRef value() {
            return SELF_REFERENCING_COLUMN_NAME;
        }
    }

    public static class PartitionsReferenceGenerationExpression extends InformationTablePartitionsExpression<BytesRef> {
        @Override
        public BytesRef value() {
            return REFERENCE_GENERATION;
        }
    }
}
