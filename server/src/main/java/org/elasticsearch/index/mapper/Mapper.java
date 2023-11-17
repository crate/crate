/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.elasticsearch.cluster.metadata.ColumnPositionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.index.analysis.IndexAnalyzers;

public abstract class Mapper implements ToXContentFragment, Iterable<Mapper> {

    public static final int NOT_TO_BE_POSITIONED = 0;

    public static class BuilderContext {
        private final Settings indexSettings;
        private final ContentPath contentPath;
        private final ColumnPositionResolver<Mapper> columnPositionResolver;

        public BuilderContext(Settings indexSettings, ContentPath contentPath) {
            Objects.requireNonNull(indexSettings, "indexSettings is required");
            this.contentPath = contentPath;
            this.indexSettings = indexSettings;
            this.columnPositionResolver = new ColumnPositionResolver<>();
        }

        public ContentPath path() {
            return this.contentPath;
        }

        public Settings indexSettings() {
            return this.indexSettings;
        }

        public void putPositionInfo(Mapper mapper, int position) {
            if (position < 0) {
                this.columnPositionResolver.addColumnToReposition(mapper.name(),
                                                                  position,
                                                                  mapper,
                                                                  (m, p) -> m.position = p,
                                                                  contentPath.currentDepth());
            }
        }

        public void updateRootObjectMapperWithPositionInfo(RootObjectMapper rootObjectMapper) {
            rootObjectMapper.updateColumnPositionResolver(this.columnPositionResolver);
        }
    }

    public abstract static class Builder<T extends Builder<?>> {

        protected String name;

        protected T builder;

        protected Builder(String name) {
            this.name = name;
        }

        protected int position;

        protected long columnOID;

        protected boolean isDropped;

        public String name() {
            return this.name;
        }

        /** Returns a newly built mapper. */
        public abstract Mapper build(BuilderContext context);

        public void position(int position) {
            this.position = position;
        }

        public void columnOID(long columnOID) {
            this.columnOID = columnOID;
        }

        public void setDropped(boolean isDropped) {
            this.isDropped = isDropped;
        }
    }

    public interface TypeParser {

        class ParserContext {

            private final MapperService mapperService;

            private final Function<String, TypeParser> typeParsers;

            public ParserContext(MapperService mapperService,
                                 Function<String, TypeParser> typeParsers) {
                this.mapperService = mapperService;
                this.typeParsers = typeParsers;
            }

            public IndexAnalyzers getIndexAnalyzers() {
                return mapperService.getIndexAnalyzers();
            }

            public MapperService mapperService() {
                return mapperService;
            }

            public TypeParser typeParser(String type) {
                return typeParsers.apply(type);
            }
        }

        Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext)
            throws MapperParsingException;
    }

    private final String simpleName;

    protected int position;

    protected long columnOID;

    protected boolean isDropped;

    protected Mapper(String simpleName, long columnOID) {
        Objects.requireNonNull(simpleName);
        if (simpleName.isEmpty()) {
            throw new IllegalArgumentException("name cannot be empty string");
        }
        this.simpleName = simpleName;
        this.columnOID = columnOID;
    }

    /** Returns the simple name, which identifies this mapper against other mappers at the same level in the mappers hierarchy
     * TODO: make this protected once Mapper and FieldMapper are merged together */
    public final String simpleName() {
        return simpleName;
    }

    /**
     * Returns the column's (field) OID this mapper is used for.
     * If no OID was assigned, {@link org.elasticsearch.cluster.metadata.Metadata#COLUMN_OID_UNASSIGNED} is returned.
     */
    public long columnOID() {
        return columnOID;
    }

    /** Returns the canonical name which uniquely identifies the mapper against other mappers in a type. */
    public abstract String name();

    /**
     * Returns a name representing the type of this mapper.
     */
    public abstract String typeName();

    /** Return the merge of {@code mergeWith} into this.
     *  Both {@code this} and {@code mergeWith} will be left unmodified. */
    public abstract Mapper merge(Mapper mergeWith);

    /**
     * Returns the max of the column positions taken by itself and its children.
     */
    public abstract int maxColumnPosition();
}
