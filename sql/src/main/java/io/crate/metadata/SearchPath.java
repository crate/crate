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

package io.crate.metadata;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * As writing fully qualified table names is usually tedious. This class models a list of schemas the system will use in
 * order to determine which table is meant by the user.
 */
public final class SearchPath implements Iterable<String> {

    private static final SearchPath PG_CATALOG_AND_DOC_PATH = new SearchPath();
    private final boolean pgCatalogIsSetExplicitly;
    private final List<String> searchPath;

    public static SearchPath createSearchPathFrom(String... schemas) {
        if (schemas == null || schemas.length == 0) {
            return new SearchPath();
        } else {
            return new SearchPath(ImmutableList.copyOf(schemas));
        }
    }

    public static SearchPath pathWithPGCatalogAndDoc() {
        return PG_CATALOG_AND_DOC_PATH;
    }

    private SearchPath() {
        pgCatalogIsSetExplicitly = false;
        searchPath = ImmutableList.of(PgCatalogSchemaInfo.NAME, Schemas.DOC_SCHEMA_NAME);
    }

    private SearchPath(ImmutableList<String> schemas) {
        assert schemas.size() > 0 : "Expecting at least one schema in the search path";
        pgCatalogIsSetExplicitly = schemas.contains(PgCatalogSchemaInfo.NAME);
        if (pgCatalogIsSetExplicitly) {
            this.searchPath = schemas;
        } else {
            ArrayList<String> completeSearchPath = new ArrayList<>(1 + schemas.size());
            completeSearchPath.add(PgCatalogSchemaInfo.NAME);
            completeSearchPath.addAll(schemas);
            this.searchPath = ImmutableList.copyOf(completeSearchPath);
        }
    }

    public String currentSchema() {
        if (pgCatalogIsSetExplicitly) {
            return searchPath.get(0);
        } else {
            return searchPath.get(1);
        }
    }

    @Override
    public Iterator<String> iterator() {
        return searchPath.iterator();
    }

    @Override
    public void forEach(Consumer<? super String> action) {
        searchPath.forEach(action);
    }

    @Override
    public Spliterator<String> spliterator() {
        return searchPath.spliterator();
    }
}
