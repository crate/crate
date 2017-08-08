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

package io.crate.operation.collect.sources;

import com.google.common.collect.FluentIterable;
import io.crate.metadata.*;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.collect.files.SqlFeatureContext;
import io.crate.operation.collect.files.SqlFeaturesIterable;
import io.crate.operation.reference.information.ColumnContext;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.StreamSupport;

public class InformationSchemaIterables implements ClusterStateListener {

    private final Schemas schemas;
    private final FluentIterable<TableInfo> tablesIterable;
    private final PartitionInfos partitionInfos;
    private final FluentIterable<ColumnContext> columnsIterable;
    private final FluentIterable<TableInfo> constraints;
    private final SqlFeaturesIterable sqlFeatures;
    private final FluentIterable<Void> keyColumnUsages;
    private final FluentIterable<Void> referentialConstraints;
    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;

    private FluentIterable<RoutineInfo> routines;
    private FluentIterable<IngestionRuleInfo> ingestionRules;

    @Inject
    public InformationSchemaIterables(final Schemas schemas,
                                      FulltextAnalyzerResolver fulltextAnalyzerResolver,
                                      ClusterService clusterService) throws IOException {
        this.schemas = schemas;
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
        tablesIterable = FluentIterable.from(schemas)
            .transformAndConcat(schema -> FluentIterable.from(schema)
                .filter(i -> !PartitionName.isPartition(i.ident().indexName())));
        partitionInfos = new PartitionInfos(clusterService);
        columnsIterable = tablesIterable.transformAndConcat(ColumnsIterable::new);

        constraints = tablesIterable.filter(i -> i != null && i.primaryKey().size() > 0);

        sqlFeatures = new SqlFeaturesIterable();
        keyColumnUsages = FluentIterable.from(Collections.emptyList());
        referentialConstraints = FluentIterable.from(Collections.emptyList());

        createMetaDataBasedIterables(clusterService.state().getMetaData());
        clusterService.addListener(this);
    }

    public Iterable<SchemaInfo> schemas() {
        return schemas;
    }

    public Iterable<TableInfo> tables() {
        return tablesIterable;
    }

    public Iterable<PartitionInfo> partitions() {
        return partitionInfos;
    }

    public Iterable<ColumnContext> columns() {
        return columnsIterable;
    }

    public Iterable<TableInfo> constraints() {
        return constraints;
    }

    public Iterable<RoutineInfo> routines() {
        return routines;
    }

    public Iterable<SqlFeatureContext> features() {
        return sqlFeatures;
    }

    public Iterable<IngestionRuleInfo> ingestionRules() {
        return ingestionRules;
    }

    public Iterable<Void> keyColumnUsageInfos() { return keyColumnUsages; }

    public Iterable<Void> referentialConstraintsInfos() { return referentialConstraints; }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.metaDataChanged() == false) {
            return;
        }
        createMetaDataBasedIterables(event.state().getMetaData());
    }

    private void createMetaDataBasedIterables(MetaData metaData) {
        RoutineInfos routineInfos = new RoutineInfos(fulltextAnalyzerResolver, metaData);
        routines = FluentIterable.from(routineInfos).filter(Objects::nonNull);
        IngestionRuleInfos ingestionRuleInfos = new IngestionRuleInfos(metaData);
        ingestionRules = FluentIterable.from(ingestionRuleInfos).filter(Objects::nonNull);
    }

    static class ColumnsIterable implements Iterable<ColumnContext> {

        private final TableInfo ti;

        ColumnsIterable(TableInfo ti) {
            this.ti = ti;
        }

        @Override
        public Iterator<ColumnContext> iterator() {
            return new ColumnsIterator(ti);
        }
    }

    static class ColumnsIterator implements Iterator<ColumnContext> {

        private final Iterator<Reference> columns;
        private final TableInfo tableInfo;
        private Short ordinal = 1;

        ColumnsIterator(TableInfo tableInfo) {
            columns = StreamSupport.stream(tableInfo.spliterator(), false)
                .filter(reference -> !reference.ident().columnIdent().isSystemColumn()
                                     && reference.valueType() != DataTypes.NOT_SUPPORTED).iterator();
            this.tableInfo = tableInfo;
        }

        @Override
        public boolean hasNext() {
            return columns.hasNext();
        }

        @Override
        public ColumnContext next() {
            if (!hasNext()) {
                throw new NoSuchElementException("Columns iterator exhausted");
            }
            Reference column = columns.next();
            if (column.ident().isColumn() == false) {
                return new ColumnContext(tableInfo, column, null);
            }
            return new ColumnContext(tableInfo, column, ordinal++);
        }
    }
}
