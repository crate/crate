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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexSettings;

public abstract class ParseContext {

    private static class FilterParseContext extends ParseContext {

        private final ParseContext in;

        private FilterParseContext(ParseContext in) {
            this.in = in;
        }

        @Override
        public DocumentMapperParser docMapperParser() {
            return in.docMapperParser();
        }

        @Override
        public boolean isWithinCopyTo() {
            return in.isWithinCopyTo();
        }

        @Override
        public IndexSettings indexSettings() {
            return in.indexSettings();
        }

        @Override
        public SourceToParse sourceToParse() {
            return in.sourceToParse();
        }

        @Override
        public ContentPath path() {
            return in.path();
        }

        @Override
        public XContentParser parser() {
            return in.parser();
        }

        @Override
        public Document doc() {
            return in.doc();
        }

        @Override
        public RootObjectMapper root() {
            return in.root();
        }

        @Override
        public DocumentMapper docMapper() {
            return in.docMapper();
        }

        @Override
        public MapperService mapperService() {
            return in.mapperService();
        }

        @Override
        public SeqNoFieldMapper.SequenceIDFields seqID() {
            return in.seqID();
        }

        @Override
        public void seqID(SeqNoFieldMapper.SequenceIDFields seqID) {
            in.seqID(seqID);
        }

        @Override
        public void addDynamicMapper(Mapper update) {
            in.addDynamicMapper(update);
        }

        @Override
        public List<Mapper> getDynamicMappers() {
            return in.getDynamicMappers();
        }
    }

    public static class InternalParseContext extends ParseContext {

        private final DocumentMapper docMapper;

        private final DocumentMapperParser docMapperParser;

        private final ContentPath path;

        private final XContentParser parser;

        private final Document document;

        private final IndexSettings indexSettings;

        private final SourceToParse sourceToParse;

        private Field version;

        private SeqNoFieldMapper.SequenceIDFields seqID;

        private final List<Mapper> dynamicMappers;

        public InternalParseContext(IndexSettings indexSettings,
                                    DocumentMapperParser docMapperParser,
                                    DocumentMapper docMapper,
                                    SourceToParse source,
                                    XContentParser parser) {
            this.indexSettings = indexSettings;
            this.docMapper = docMapper;
            this.docMapperParser = docMapperParser;
            this.path = new ContentPath(0);
            this.parser = parser;
            this.document = new Document();
            this.version = null;
            this.sourceToParse = source;
            this.dynamicMappers = new ArrayList<>();
        }

        @Override
        public DocumentMapperParser docMapperParser() {
            return this.docMapperParser;
        }

        @Override
        public IndexSettings indexSettings() {
            return this.indexSettings;
        }

        @Override
        public SourceToParse sourceToParse() {
            return this.sourceToParse;
        }

        @Override
        public ContentPath path() {
            return this.path;
        }

        @Override
        public XContentParser parser() {
            return this.parser;
        }

        @Override
        public Document doc() {
            return this.document;
        }

        @Override
        public RootObjectMapper root() {
            return docMapper.root();
        }

        @Override
        public DocumentMapper docMapper() {
            return this.docMapper;
        }

        @Override
        public MapperService mapperService() {
            return docMapperParser.mapperService;
        }

        @Override
        public SeqNoFieldMapper.SequenceIDFields seqID() {
            return this.seqID;
        }

        @Override
        public void seqID(SeqNoFieldMapper.SequenceIDFields seqID) {
            this.seqID = seqID;
        }

        @Override
        public void addDynamicMapper(Mapper mapper) {
            dynamicMappers.add(mapper);
        }

        @Override
        public List<Mapper> getDynamicMappers() {
            return dynamicMappers;
        }
    }

    public abstract DocumentMapperParser docMapperParser();

    /**
     * Return a new context that will be within a copy-to operation.
     */
    public final ParseContext createCopyToContext() {
        return new FilterParseContext(this) {
            @Override
            public boolean isWithinCopyTo() {
                return true;
            }
        };
    }

    public boolean isWithinCopyTo() {
        return false;
    }

    /**
     * Return a new context that will have the provided path.
     */
    public final ParseContext overridePath(final ContentPath path) {
        return new FilterParseContext(this) {
            @Override
            public ContentPath path() {
                return path;
            }
        };
    }

    public abstract IndexSettings indexSettings();

    public abstract SourceToParse sourceToParse();

    public abstract ContentPath path();

    public abstract XContentParser parser();

    public abstract Document doc();

    public abstract RootObjectMapper root();

    public abstract DocumentMapper docMapper();

    public abstract MapperService mapperService();

    public abstract SeqNoFieldMapper.SequenceIDFields seqID();

    public abstract void seqID(SeqNoFieldMapper.SequenceIDFields seqID);

    /**
     * Add a new mapper dynamically created while parsing.
     */
    public abstract void addDynamicMapper(Mapper update);

    /**
     * Get dynamic mappers created while parsing.
     */
    public abstract List<Mapper> getDynamicMappers();
}
