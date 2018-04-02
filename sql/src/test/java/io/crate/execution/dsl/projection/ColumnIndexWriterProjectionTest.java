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

package io.crate.execution.dsl.projection;

import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.RelationName;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Settings;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertThat;

public class ColumnIndexWriterProjectionTest {

    private RelationName relationName = new RelationName("dummy", "table");

    @Test
    public void testTargetColumnRefsAndSymbolsAreCorrectAfterExclusionOfPartitionColumns() {
        /*
         *  pk:             [ymd, domain, area, isp]
         *  partitioned by: [ymd, isp]
         *  insert into t   (ymd, domain, area, isp, h) select ...
         *
         *  We don't write PartitionedBy columns, so they're excluded from the targetColumns:
         *
         *  expected targetRefs:      [ domain, area, h ]
         *  expected column symbols:  [ ic1,   ic2, ic4 ]
         */
        ColumnIdent ymd = new ColumnIdent("ymd");
        ColumnIdent domain = new ColumnIdent("domain");
        ColumnIdent area = new ColumnIdent("area");
        ColumnIdent isp = new ColumnIdent("isp");
        ColumnIdent h = new ColumnIdent("h");
        Reference ymdRef = partitionRef(ymd, DataTypes.STRING);
        Reference domainRef = ref(domain, DataTypes.STRING);
        Reference areaRef = ref(area, DataTypes.STRING);
        Reference ispRef = partitionRef(isp, DataTypes.STRING);
        Reference hRef = ref(h, DataTypes.INTEGER);
        List<ColumnIdent> primaryKeys = Arrays.asList(ymd, domain, area, isp);
        List<Reference> targetColumns = Arrays.asList(ymdRef, domainRef, areaRef, ispRef, hRef);
        InputColumns.SourceSymbols targetColsCtx = new InputColumns.SourceSymbols(targetColumns);
        List<Symbol> primaryKeySymbols = InputColumns.create(
            Arrays.asList(ymdRef, domainRef, areaRef, ispRef), targetColsCtx);
        List<ColumnIdent> partitionedByColumns = Arrays.asList(ymd, isp);
        List<Symbol> partitionedBySymbols = InputColumns.create(Arrays.asList(ymdRef, ispRef), targetColsCtx);

        ColumnIndexWriterProjection projection = new ColumnIndexWriterProjection(
            relationName,
            null,
            primaryKeys,
            targetColumns,
            false,
            null,
            primaryKeySymbols,
            partitionedByColumns,
            partitionedBySymbols,
            null,
            null,
            Settings.EMPTY,
            true
        );

        assertThat(projection.columnReferences(), Matchers.is(Arrays.asList(
            domainRef, areaRef, hRef)
        ));
        assertThat(projection.columnSymbols(), Matchers.is(Arrays.asList(
            new InputColumn(1, DataTypes.STRING),
            new InputColumn(2, DataTypes.STRING),
            new InputColumn(4, DataTypes.INTEGER))));
    }

    private Reference ref(ColumnIdent column, DataType type) {
        return new Reference(new ReferenceIdent(relationName, column), RowGranularity.DOC, type);
    }

    private Reference partitionRef(ColumnIdent column, DataType type) {
        return new Reference(new ReferenceIdent(relationName, column), RowGranularity.PARTITION, type);
    }
}
