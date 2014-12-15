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
package io.crate.metadata;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import io.crate.core.StringUtils;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.types.DataTypes;
import org.elasticsearch.common.lucene.BytesRefs;

import javax.annotation.Nullable;
import java.util.*;

public class TablePartitionInfos implements Iterable<TablePartitionInfo> {

    private Iterable<TableInfo> tableInfos;

    public TablePartitionInfos(Iterable<TableInfo> tableInfos) {
        this.tableInfos = tableInfos;
    }

    @Override
    public Iterator<TablePartitionInfo> iterator() {
        return FluentIterable.from(tableInfos).filter(new Predicate<TableInfo>() {
            @Override
            public boolean apply(@Nullable TableInfo input) {
                return (input instanceof DocTableInfo) && input.isPartitioned() && input.partitions().size() > 0;
            }
        }).transformAndConcat(
                new Function<TableInfo, Iterable<? extends TablePartitionInfo>>() {
                    @Nullable
                    @Override
                    public Iterable<? extends TablePartitionInfo> apply(@Nullable TableInfo input) {
                        return new TablePartitionInfosIterable((DocTableInfo) input);
                    }
                }
        ).iterator();
    }

    public static class TablePartitionInfosIterable
            implements Iterable<TablePartitionInfo> {

        private DocTableInfo info;

        public TablePartitionInfosIterable(DocTableInfo info) {
            this.info = info;
        }

        @Override
        public Iterator<TablePartitionInfo> iterator() {
            String tableName = info.ident().name();
            String schemaName = info.ident().schema();
            if (schemaName == null) {
                schemaName = ReferenceInfos.DEFAULT_SCHEMA_NAME;
            }

            List<TablePartitionInfo> tablePartitionInfos = new ArrayList<>();
            for (PartitionName pn : info.partitions()) {
                Map<String, Object> values = new HashMap<>();
                String partitionIdent = pn.ident();
                for (int i = 0; i < info.partitionedByColumns().size(); i++) {
                    // column
                    ReferenceInfo referenceInfo = info.partitionedByColumns().get(i);
                    String partitionCol = StringUtils.dottedToSqlPath(referenceInfo.ident().columnIdent().fqn());

                    // value
                    Object value = BytesRefs.toString(pn.values().get(i));
                    if (!referenceInfo.type().equals(DataTypes.STRING)) {
                        value = referenceInfo.type().value(value);
                    }

                    // column -> value
                    values.put(partitionCol, value);
                }
                TablePartitionInfo tpi = new TablePartitionInfo(tableName, schemaName,
                        partitionIdent, values);
                tablePartitionInfos.add(tpi);
            }

            return FluentIterable.from(tablePartitionInfos).iterator();
        }

    }
}
