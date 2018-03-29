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
import io.crate.metadata.RelationName;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.execution.engine.collect.files.SqlFeatureContext;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.types.DataTypes;

import java.util.Map;


public class InformationSqlFeaturesTableInfo extends InformationTableInfo {

    public static final String NAME = "sql_features";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    public static class Columns {
        static final ColumnIdent FEATURE_ID = new ColumnIdent("feature_id");
        static final ColumnIdent FEATURE_NAME = new ColumnIdent("feature_name");
        static final ColumnIdent SUB_FEATURE_ID = new ColumnIdent("sub_feature_id");
        static final ColumnIdent SUB_FEATURE_NAME = new ColumnIdent("sub_feature_name");
        static final ColumnIdent IS_SUPPORTED = new ColumnIdent("is_supported");
        static final ColumnIdent IS_VERIFIED_BY = new ColumnIdent("is_verified_by");
        static final ColumnIdent COMMENTS = new ColumnIdent("comments");
    }

    private static ColumnRegistrar columnRegistrar() {
        return new ColumnRegistrar(IDENT, RowGranularity.DOC)
            .register(Columns.FEATURE_ID, DataTypes.STRING)
            .register(Columns.FEATURE_NAME, DataTypes.STRING)
            .register(Columns.SUB_FEATURE_ID, DataTypes.STRING)
            .register(Columns.SUB_FEATURE_NAME, DataTypes.STRING)
            .register(Columns.IS_SUPPORTED, DataTypes.BOOLEAN)
            .register(Columns.IS_VERIFIED_BY, DataTypes.STRING)
            .register(Columns.COMMENTS, DataTypes.STRING);
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<SqlFeatureContext>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<SqlFeatureContext>>builder()
            .put(Columns.FEATURE_ID,
                () -> RowContextCollectorExpression.objToBytesRef(SqlFeatureContext::getFeatureId))
            .put(Columns.FEATURE_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(SqlFeatureContext::getFeatureName))
            .put(Columns.SUB_FEATURE_ID,
                () -> RowContextCollectorExpression.objToBytesRef(SqlFeatureContext::getSubFeatureId))
            .put(Columns.SUB_FEATURE_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(SqlFeatureContext::getSubFeatureName))
            .put(Columns.IS_SUPPORTED,
                () -> RowContextCollectorExpression.forFunction(SqlFeatureContext::isSupported))
            .put(Columns.IS_VERIFIED_BY,
                () -> RowContextCollectorExpression.objToBytesRef(SqlFeatureContext::getIsVerifiedBy))
            .put(Columns.COMMENTS,
                () -> RowContextCollectorExpression.objToBytesRef(SqlFeatureContext::getComments))
            .build();
    }

    InformationSqlFeaturesTableInfo() {
        super(
            IDENT,
            columnRegistrar(),
            ImmutableList.of(Columns.FEATURE_ID, Columns.FEATURE_NAME,
                Columns.SUB_FEATURE_ID, Columns.SUB_FEATURE_NAME,
                Columns.IS_SUPPORTED, Columns.IS_VERIFIED_BY)
        );
    }
}
