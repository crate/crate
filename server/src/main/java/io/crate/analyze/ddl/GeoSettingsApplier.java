/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.analyze.ddl;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.DistanceUnit;
import org.jetbrains.annotations.Nullable;

import io.crate.sql.tree.GenericProperties;

public class GeoSettingsApplier {

    private static final Set<String> SUPPORTED_OPTIONS = Set.of(
        "precision", "distance_error_pct", "tree_levels");

    public static void applySettings(Map<String, Object> mapping, GenericProperties<Object> properties, @Nullable String geoTree) {
        Settings geoSettings = Settings.builder().put(properties).build();
        validate(geoSettings);
        if (geoTree != null) {
            mapping.put("tree", geoTree);
        }
        applyPrecision(mapping, geoSettings);
        applyDistanceErrorPct(mapping, geoSettings);
    }

    private static void applyDistanceErrorPct(Map<String, Object> mapping, Settings geoSettings) {
        try {
            Float errorPct = geoSettings.getAsFloat("distance_error_pct", null);
            if (errorPct != null) {
                mapping.put("distance_error_pct", errorPct);
            }
        } catch (SettingsException e) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Value '%s' of setting distance_error_pct is not a float value", geoSettings.get("distance_error_pct"))
            );
        }
    }

    private static void applyPrecision(Map<String, Object> mapping, Settings geoSettings) {
        String precision = geoSettings.get("precision");
        if (precision == null) {
            Integer treeLevels = geoSettings.getAsInt("tree_levels", null);
            if (treeLevels != null) {
                mapping.put("tree_levels", treeLevels);
            }
        } else {
            try {
                DistanceUnit.parse(precision, DistanceUnit.DEFAULT, DistanceUnit.DEFAULT);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Value '%s' of setting precision is not a valid distance unit", precision)
                );
            }
            mapping.put("precision", precision);
        }
    }

    private static void validate(Settings geoSettings) {
        for (String setting : geoSettings.names()) {
            if (!SUPPORTED_OPTIONS.contains(setting)) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Setting \"%s\" ist not supported on geo_shape index", setting));
            }
        }
    }
}
