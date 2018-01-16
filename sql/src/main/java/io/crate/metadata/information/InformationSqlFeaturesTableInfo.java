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
import com.google.common.collect.ImmutableSortedMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.execution.engine.collect.files.SqlFeatureContext;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Map;


public class InformationSqlFeaturesTableInfo extends InformationTableInfo {

    public static final String NAME = "sql_features";
    public static final TableIdent IDENT = new TableIdent(InformationSchemaInfo.NAME, NAME);

    public static class Columns {
        public static final ColumnIdent FEATURE_ID = new ColumnIdent("feature_id");
        public static final ColumnIdent FEATURE_NAME = new ColumnIdent("feature_name");
        public static final ColumnIdent SUB_FEATURE_ID = new ColumnIdent("sub_feature_id");
        public static final ColumnIdent SUB_FEATURE_NAME = new ColumnIdent("sub_feature_name");
        public static final ColumnIdent IS_SUPPORTED = new ColumnIdent("is_supported");
        public static final ColumnIdent IS_VERIFIED_BY = new ColumnIdent("is_verified_by");
        public static final ColumnIdent COMMENTS = new ColumnIdent("comments");
    }

    public static class References {
        public static final Reference FEATURE_ID = createRef(Columns.FEATURE_ID, DataTypes.STRING);
        public static final Reference FEATURE_NAME = createRef(Columns.FEATURE_NAME, DataTypes.STRING);
        public static final Reference SUB_FEATURE_ID = createRef(Columns.SUB_FEATURE_ID, DataTypes.STRING);
        public static final Reference SUB_FEATURE_NAME = createRef(Columns.SUB_FEATURE_NAME, DataTypes.STRING);
        public static final Reference IS_SUPPORTED = createRef(Columns.IS_SUPPORTED, DataTypes.BOOLEAN);
        public static final Reference IS_VERIFIED_BY = createRef(Columns.IS_VERIFIED_BY, DataTypes.STRING);
        public static final Reference COMMENTS = createRef(Columns.COMMENTS, DataTypes.STRING);
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<SqlFeatureContext>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<SqlFeatureContext>>builder()
            .put(InformationSqlFeaturesTableInfo.Columns.FEATURE_ID,
                () -> RowContextCollectorExpression.objToBytesRef(SqlFeatureContext::getFeatureId))
            .put(InformationSqlFeaturesTableInfo.Columns.FEATURE_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(SqlFeatureContext::getFeatureName))
            .put(InformationSqlFeaturesTableInfo.Columns.SUB_FEATURE_ID,
                () -> RowContextCollectorExpression.objToBytesRef(SqlFeatureContext::getSubFeatureId))
            .put(InformationSqlFeaturesTableInfo.Columns.SUB_FEATURE_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(SqlFeatureContext::getSubFeatureName))
            .put(InformationSqlFeaturesTableInfo.Columns.IS_SUPPORTED,
                () -> RowContextCollectorExpression.forFunction(SqlFeatureContext::isSupported))
            .put(InformationSqlFeaturesTableInfo.Columns.IS_VERIFIED_BY,
                () -> RowContextCollectorExpression.objToBytesRef(SqlFeatureContext::getIsVerifiedBy))
            .put(InformationSqlFeaturesTableInfo.Columns.COMMENTS,
                () -> RowContextCollectorExpression.objToBytesRef(SqlFeatureContext::getComments))
            .build();
    }

    private static Reference createRef(ColumnIdent columnIdent, DataType dataType) {
        return new Reference(new ReferenceIdent(IDENT, columnIdent), RowGranularity.DOC, dataType);
    }

    protected InformationSqlFeaturesTableInfo() {
        super(
            IDENT,
            ImmutableList.of(Columns.FEATURE_ID,
                Columns.FEATURE_NAME,
                Columns.SUB_FEATURE_ID,
                Columns.SUB_FEATURE_NAME,
                Columns.IS_SUPPORTED,
                Columns.IS_VERIFIED_BY,
                Columns.COMMENTS),
            ImmutableSortedMap.<ColumnIdent, Reference>naturalOrder()
                .put(Columns.FEATURE_ID, References.FEATURE_ID)
                .put(Columns.FEATURE_NAME, References.FEATURE_NAME)
                .put(Columns.SUB_FEATURE_ID, References.SUB_FEATURE_ID)
                .put(Columns.SUB_FEATURE_NAME, References.SUB_FEATURE_NAME)
                .put(Columns.IS_SUPPORTED, References.IS_SUPPORTED)
                .put(Columns.IS_VERIFIED_BY, References.IS_VERIFIED_BY)
                .put(Columns.COMMENTS, References.COMMENTS)
                .build()
        );
    }

}
