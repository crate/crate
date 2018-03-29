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

package io.crate.metadata.information;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IngestionRuleInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.types.DataTypes;

import java.util.Map;


public class InformationSchemaIngestionRulesTableInfo extends InformationTableInfo {

    public static final String NAME = "ingestion_rules";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    public static class Columns {
        static final ColumnIdent RULE_NAME = new ColumnIdent("rule_name");
        static final ColumnIdent SOURCE_IDENT = new ColumnIdent("source_ident");
        static final ColumnIdent TARGET_TABLE = new ColumnIdent("target_table");
        static final ColumnIdent CONDITION = new ColumnIdent("condition");
    }

    private static ColumnRegistrar columnRegistrar() {
        return new ColumnRegistrar(IDENT, RowGranularity.DOC)
            .register(Columns.RULE_NAME, DataTypes.STRING)
            .register(Columns.SOURCE_IDENT, DataTypes.STRING)
            .register(Columns.TARGET_TABLE, DataTypes.STRING)
            .register(Columns.CONDITION, DataTypes.STRING);
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<IngestionRuleInfo>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<IngestionRuleInfo>>builder()
            .put(Columns.RULE_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(IngestionRuleInfo::getName))
            .put(Columns.SOURCE_IDENT,
                () -> RowContextCollectorExpression.objToBytesRef(IngestionRuleInfo::getSource))
            .put(Columns.TARGET_TABLE,
                () -> RowContextCollectorExpression.objToBytesRef(IngestionRuleInfo::getTarget))
            .put(Columns.CONDITION,
                () -> RowContextCollectorExpression.objToBytesRef(IngestionRuleInfo::getCondition))
            .build();
    }

    InformationSchemaIngestionRulesTableInfo() {
        super(
            IDENT,
            columnRegistrar(),
            ImmutableList.of(Columns.RULE_NAME)
        );
    }
}
