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

package io.crate.analyze;

import com.google.common.collect.ImmutableMap;
import io.crate.blob.v2.BlobIndices;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.sql.tree.Expression;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.Locale;

public class CreateBlobTablePropertiesAnalysis extends TablePropertiesAnalysis {

    /**
     * This var is used internally here only! The public property is "blobs_path".
     * The ES index setting property is defined at BlobIndices.SETTING_INDEX_BLOBS_PATH
     */
    private static final String PROPERTIES_PATH = "index.blobs_path";

    private static final ImmutableMap<String, SettingsApplier> supportedProperties =
            ImmutableMap.<String, SettingsApplier>builder()
                    .put(NUMBER_OF_REPLICAS, new NumberOfReplicasSettingApplier())
                    .put(PROPERTIES_PATH, new BlobPathSettingApplier())
                    .build();

    @Override
    protected ImmutableMap<String, SettingsApplier> supportedProperties() {
        return supportedProperties;
    }

    @Override
    protected ColumnPolicy defaultColumnPolicy() {
        return ColumnPolicy.STRICT;
    }

    private static class BlobPathSettingApplier implements SettingsApplier {

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder,
                          Object[] parameters,
                          Expression expression) {
            String blobPath;
            try {
                blobPath = SafeExpressionToStringVisitor.convert(expression, parameters);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "Invalid value for argument '%s'",
                                denormalizeKey(PROPERTIES_PATH)), e);
            }
            settingsBuilder.put(BlobIndices.SETTING_INDEX_BLOBS_PATH, blobPath);
        }

        @Override
        public Settings getDefault() {
            return ImmutableSettings.EMPTY;
        }

        @Override
        public void applyValue(ImmutableSettings.Builder settingsBuilder, Object value) {
            throw new UnsupportedOperationException("Not supported");
        }
    }

}
