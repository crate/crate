/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package io.crate.analyze;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;

import org.elasticsearch.common.settings.Settings;

import io.crate.data.Row;
import io.crate.metadata.MaterializedViewMetadata;
import io.crate.metadata.SearchPath;
import io.crate.sql.SqlFormatter;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Literal;
import io.crate.sql.tree.Query;

public record AnalyzedMaterializedViewDefinition(Query query, SearchPath searchPath, String owner) {

    public void addTo(Settings.Builder settings, Row params) {
        List<Expression> parameterValues = new ArrayList<>(params.numColumns());
        for (int i = 0; i < params.numColumns(); i++) {
            parameterValues.add(Literal.fromObject(params.get(i)));
        }
        settings.put(MaterializedViewMetadata.DEFINITION, SqlFormatter.formatSql(query, parameterValues));
        settings.putList(
            MaterializedViewMetadata.SEARCH_PATH,
            StreamSupport.stream(searchPath.showPath().spliterator(), false).toList()
        );
        settings.put(MaterializedViewMetadata.OWNER, owner);
    }
}
