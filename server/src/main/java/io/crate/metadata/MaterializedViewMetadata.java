/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package io.crate.metadata;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

public final class MaterializedViewMetadata {

    public static final String PREFIX = "index.crate.materialized_view.";
    public static final String DEFINITION = PREFIX + "definition";
    public static final String SEARCH_PATH = PREFIX + "search_path";
    public static final String OWNER = PREFIX + "owner";
    public static final Setting<Settings> SETTINGS = Setting.groupSetting(
        PREFIX,
        Setting.Property.IndexScope
    );

    private MaterializedViewMetadata() {
    }

    public static boolean isMaterialized(Settings settings) {
        return settings.get(DEFINITION) != null;
    }

    public static String definition(Settings settings) {
        String definition = settings.get(DEFINITION);
        if (definition == null) {
            throw new IllegalArgumentException("Relation is not a materialized view");
        }
        return definition;
    }

    public static SearchPath searchPath(Settings settings, SearchPath fallback) {
        var schemas = settings.getAsList(SEARCH_PATH);
        return schemas.isEmpty()
            ? fallback
            : SearchPath.createSearchPathFrom(schemas.toArray(String[]::new));
    }

    public static String owner(Settings settings, String fallback) {
        return settings.get(OWNER, fallback);
    }
}
