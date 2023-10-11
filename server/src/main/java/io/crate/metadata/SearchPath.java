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

package io.crate.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;

/**
 * Writing fully qualified table names is usually tedious.
 * This class models a list of schemas the system will use
 * in order to determine which table is meant by the user.
 */
public final class SearchPath implements Iterable<String>, Writeable {

    private static final SearchPath PG_CATALOG_AND_DOC_PATH = new SearchPath();
    private final boolean pgCatalogIsSetExplicitly;
    private final List<String> searchPath;

    public static SearchPath createSearchPathFrom(String... schemas) {
        if (schemas == null || schemas.length == 0) {
            return new SearchPath();
        } else {
            return new SearchPath(List.of(schemas));
        }
    }

    public static SearchPath pathWithPGCatalogAndDoc() {
        return PG_CATALOG_AND_DOC_PATH;
    }

    public static SearchPath createSearchPathFrom(StreamInput in) throws IOException {
        final boolean pgCatalogIsSetExplicitly = in.readBoolean();
        final List<String> searchPath = in.readList(StreamInput::readString);
        return new SearchPath(pgCatalogIsSetExplicitly, searchPath);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(pgCatalogIsSetExplicitly);
        out.writeStringCollection(searchPath);
    }

    private SearchPath() {
        this.pgCatalogIsSetExplicitly = false;
        this.searchPath = List.of(PgCatalogSchemaInfo.NAME, Schemas.DOC_SCHEMA_NAME);
    }

    private SearchPath(boolean pgCatalogIsSetExplicitly, List<String> searchPath) {
        this.pgCatalogIsSetExplicitly = pgCatalogIsSetExplicitly;
        this.searchPath = searchPath;
    }

    private SearchPath(List<String> schemas) {
        assert schemas.size() > 0 : "Expecting at least one schema in the search path";
        this.pgCatalogIsSetExplicitly = schemas.contains(PgCatalogSchemaInfo.NAME);
        if (pgCatalogIsSetExplicitly) {
            this.searchPath = schemas;
        } else {
            ArrayList<String> completeSearchPath = new ArrayList<>(1 + schemas.size());
            completeSearchPath.add(PgCatalogSchemaInfo.NAME);
            completeSearchPath.addAll(schemas);
            this.searchPath = List.copyOf(completeSearchPath);
        }
    }

    public String currentSchema() {
        if (pgCatalogIsSetExplicitly) {
            return searchPath.get(0);
        } else {
            return searchPath.get(1);
        }
    }

    /**
     * Path to show in `SHOW search_path`
     *
     * @return the search path excluding pg_catalog unless it was set explicitly
     */
    public Iterable<String> showPath() {
        if (pgCatalogIsSetExplicitly) {
            return searchPath;
        } else {
            return searchPath.subList(1, searchPath.size());
        }
    }

    @Override
    public Iterator<String> iterator() {
        return searchPath.iterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SearchPath that = (SearchPath) o;
        return pgCatalogIsSetExplicitly == that.pgCatalogIsSetExplicitly &&
               Objects.equals(searchPath, that.searchPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pgCatalogIsSetExplicitly, searchPath);
    }
}
