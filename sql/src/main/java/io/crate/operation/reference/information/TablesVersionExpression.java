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

package io.crate.operation.reference.information;

import io.crate.Version;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.reference.RowCollectNestedObjectExpression;

import javax.annotation.Nullable;
import java.util.Map;

class TablesVersionExpression extends RowCollectNestedObjectExpression<TableInfo> {

    TablesVersionExpression() {
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(Version.Properties.CREATED.toString(),
            new TableVersionCreatedExpression(Version.Properties.CREATED.toString()));
        childImplementations.put(Version.Properties.UPGRADED.toString(),
            new TableVersionCreatedExpression(Version.Properties.UPGRADED.toString()));
    }

    @Override
    public Map<String, Object> value() {
        Map<String, Object> map = super.value();
        return getValuesMap(map);
    }

    static class TableVersionCreatedExpression extends RowCollectNestedObjectExpression<TableInfo> {

        TableVersionCreatedExpression(String property) {
            addChildImplementations(property);
        }

        private void addChildImplementations(String property) {
            childImplementations.put(Version.CRATEDB_VERSION_KEY,
                new TableDetailedVersionExpression(property, Version.CRATEDB_VERSION_KEY));
            childImplementations.put(Version.ES_VERSION_KEY,
                new TableDetailedVersionExpression(property, Version.ES_VERSION_KEY));
        }

        @Override
        public Map<String, Object> value() {
            Map<String, Object> map = super.value();
            return getValuesMap(map);
        }
    }

    static class TableDetailedVersionExpression extends RowContextCollectorExpression<TableInfo, Object> {

        private final String property;
        private final String dbKey;

        TableDetailedVersionExpression(String property, String dbKey) {
            this.property = property;
            this.dbKey = dbKey;
        }

        @Override
        public Object value() {
            Version version;
            if (Version.Properties.CREATED.toString().equals(property)) {
                version = row.versionCreated();
            } else {
                version = row.versionUpgraded();
            }

            return version == null ? null : Version.toMap(version).get(dbKey);
        }
    }

    @Nullable
    private static Map<String, Object> getValuesMap(Map<String, Object> map) {
        for (Object value : map.values()) {
            if (value != null) {
                return map;
            }
        }
        return null;
    }
}
