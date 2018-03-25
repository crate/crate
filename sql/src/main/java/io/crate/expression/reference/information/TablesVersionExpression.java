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

package io.crate.expression.reference.information;

import io.crate.Version;
import io.crate.core.collections.Maps;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.table.TableInfo;
import io.crate.expression.reference.RowCollectNestedObjectExpression;

import java.util.Map;

public class TablesVersionExpression extends RowCollectNestedObjectExpression<RelationInfo> {

    public TablesVersionExpression() {
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(Version.Property.CREATED.toString(),
            new TableDetailedVersionExpression(Version.Property.CREATED));
        childImplementations.put(Version.Property.UPGRADED.toString(),
            new TableDetailedVersionExpression(Version.Property.UPGRADED));
    }

    @Override
    public Map<String, Object> value() {
        Map<String, Object> map = super.value();
        return Maps.mapOrNullIfNullValues(map);
    }

    static class TableDetailedVersionExpression extends RowCollectNestedObjectExpression<TableInfo> {

        TableDetailedVersionExpression(Version.Property property) {
            addChildImplementations(property);
        }

        private void addChildImplementations(Version.Property property) {
            childImplementations.put(Version.CRATEDB_VERSION_KEY, new TableCrateVersionExpression(property));
            childImplementations.put(Version.ES_VERSION_KEY, new TableESVersionExpression(property));
        }

        @Override
        public Map<String, Object> value() {
            Map<String, Object> map = super.value();
            return Maps.mapOrNullIfNullValues(map);
        }
    }

    static class TableCrateVersionExpression extends RowContextCollectorExpression<TableInfo, Object> {

        private final Version.Property property;

        TableCrateVersionExpression(Version.Property property) {
            this.property = property;
        }

        @Override
        public Object value() {
            Version version = TableExpressions.getVersion(row, property);
            return version == null ? null : version.number();
        }
    }

    static class TableESVersionExpression extends RowContextCollectorExpression<TableInfo, Object> {

        private final Version.Property property;

        TableESVersionExpression(Version.Property property) {
            this.property = property;
        }

        @Override
        public Object value() {
            Version version = TableExpressions.getVersion(row, property);
            return version == null ? null : version.esVersion.toString();
        }
    }
}
