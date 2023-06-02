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

import static io.crate.server.xcontent.XContentMapValues.nodeBooleanValue;
import static io.crate.server.xcontent.XContentMapValues.nodeIntegerValue;
import static io.crate.server.xcontent.XContentMapValues.nodeLongValue;
import static io.crate.server.xcontent.XContentMapValues.nodeStringValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.index.analysis.NamedAnalyzer;

import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

import io.crate.server.xcontent.XContentMapValues;

public class TypeParsers {

    public static final String DOC_VALUES = "doc_values";
    public static final String INDEX_OPTIONS_DOCS = "docs";
    public static final String INDEX_OPTIONS_FREQS = "freqs";
    public static final String INDEX_OPTIONS_POSITIONS = "positions";
    public static final String INDEX_OPTIONS_OFFSETS = "offsets";

    private static void parseAnalyzersAndTermVectors(TextFieldMapper.Builder builder,
                                                     String name,
                                                     Map<String, Object> fieldNode,
                                                     Mapper.TypeParser.ParserContext parserContext) {
        NamedAnalyzer indexAnalyzer = null;
        NamedAnalyzer searchAnalyzer = null;
        NamedAnalyzer searchQuoteAnalyzer = null;

        for (Iterator<Map.Entry<String, Object>> iterator = fieldNode.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry<String, Object> entry = iterator.next();
            final String propName = entry.getKey();
            final Object propNode = entry.getValue();
            if (propName.equals("term_vector")) {
                parseTermVector(name, propNode.toString(), builder);
                iterator.remove();
            } else if (propName.equals("store_term_vectors")) {
                builder.storeTermVectors(nodeBooleanValue(propNode, name + ".store_term_vectors"));
                iterator.remove();
            } else if (propName.equals("store_term_vector_offsets")) {
                builder.storeTermVectorOffsets(nodeBooleanValue(propNode, name + ".store_term_vector_offsets"));
                iterator.remove();
            } else if (propName.equals("store_term_vector_positions")) {
                builder.storeTermVectorPositions(nodeBooleanValue(name + ".store_term_vector_positions"));
                iterator.remove();
            } else if (propName.equals("store_term_vector_payloads")) {
                builder.storeTermVectorPayloads(nodeBooleanValue(name + ".store_term_vector_payloads"));
                iterator.remove();
            } else if (propName.equals("analyzer")) {
                NamedAnalyzer analyzer = parserContext.getIndexAnalyzers().get(propNode.toString());
                if (analyzer == null) {
                    throw new MapperParsingException("analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                }
                indexAnalyzer = analyzer;
                iterator.remove();
            } else if (propName.equals("search_analyzer")) {
                NamedAnalyzer analyzer = parserContext.getIndexAnalyzers().get(propNode.toString());
                if (analyzer == null) {
                    throw new MapperParsingException("analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                }
                searchAnalyzer = analyzer;
                iterator.remove();
            } else if (propName.equals("search_quote_analyzer")) {
                NamedAnalyzer analyzer = parserContext.getIndexAnalyzers().get(propNode.toString());
                if (analyzer == null) {
                    throw new MapperParsingException("analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                }
                searchQuoteAnalyzer = analyzer;
                iterator.remove();
            }
        }

        if (indexAnalyzer == null && searchAnalyzer != null) {
            throw new MapperParsingException("analyzer on field [" + name + "] must be set when search_analyzer is set");
        }

        if (searchAnalyzer == null && searchQuoteAnalyzer != null) {
            throw new MapperParsingException("analyzer and search_analyzer on field [" + name +
                "] must be set when search_quote_analyzer is set");
        }

        if (searchAnalyzer == null) {
            searchAnalyzer = indexAnalyzer;
        }

        if (searchQuoteAnalyzer == null) {
            searchQuoteAnalyzer = searchAnalyzer;
        }

        if (indexAnalyzer != null) {
            builder.indexAnalyzer(indexAnalyzer);
        }
        if (searchAnalyzer != null) {
            builder.searchAnalyzer(searchAnalyzer);
        }
        if (searchQuoteAnalyzer != null) {
            builder.searchQuoteAnalyzer(searchQuoteAnalyzer);
        }
    }

    public static void parseNorms(TextFieldMapper.Builder builder, String fieldName, Object propNode) {
        builder.omitNorms(XContentMapValues.nodeBooleanValue(propNode, fieldName + ".norms") == false);
    }

    /**
     * Parse text field attributes. In addition to {@link #parseField common attributes}
     * this will parse analysis and term-vectors related settings.
     */
    public static void parseTextField(TextFieldMapper.Builder builder,
                                      String name,
                                      Map<String, Object> fieldNode,
                                      Mapper.TypeParser.ParserContext parserContext) {
        parseField(builder, name, fieldNode);
        parseAnalyzersAndTermVectors(builder, name, fieldNode, parserContext);
        for (Iterator<Map.Entry<String, Object>> iterator = fieldNode.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<String, Object> entry = iterator.next();
            final String propName = entry.getKey();
            final Object propNode = entry.getValue();
            if ("norms".equals(propName)) {
                parseNorms(builder, name, propNode);
                iterator.remove();
            } else if (propName.equals("sources")) {
                List<String> sources = new ArrayList<>();
                assert propNode instanceof List<?> : "index sources must be a list";
                List<?> nodes = (List<?>) propNode;
                for (Object node : nodes) {
                    sources.add(nodeStringValue(node, null));
                }
                builder.sources(sources);
                iterator.remove();
            }
        }
    }

    /**
     * Parse common field attributes such as {@code doc_values} or {@code store}.
     */
    public static void parseField(FieldMapper.Builder<?> builder,
                                  String name,
                                  Map<String, Object> fieldNode) {
        for (Iterator<Map.Entry<String, Object>> iterator = fieldNode.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry<String, Object> entry = iterator.next();
            final String propName = entry.getKey();
            final Object propNode = entry.getValue();
            if (propNode == null) {
                /*
                 * No properties are allowed to have null. So we catch it here and tell the user something useful rather
                 * than send them a null pointer exception later.
                 */
                throw new MapperParsingException("[" + propName + "] must not have a [null] value");
            }
            if (propName.equals("store")) {
                builder.store(nodeBooleanValue(propNode, name + ".store"));
                iterator.remove();
            } else if (propName.equals("index")) {
                builder.index(nodeBooleanValue(propNode, name + ".index"));
                iterator.remove();
            } else if (propName.equals(DOC_VALUES)) {
                builder.docValues(nodeBooleanValue(propNode, name + '.' + DOC_VALUES));
                iterator.remove();
            } else if (propName.equals("index_options")) {
                builder.indexOptions(nodeIndexOptionValue(propNode));
                iterator.remove();
            } else if (propName.equals("copy_to")) {
                parseCopyFields(propNode, builder);
                iterator.remove();
            } else if (propName.equals("position")) {
                builder.position(nodeIntegerValue(propNode));
                iterator.remove();
            } else if (propName.equals("default_expr")) {
                builder.defaultExpression(nodeStringValue(propNode, null));
                iterator.remove();
            } else if (propName.equals("oid")) {
                builder.columnOID(nodeLongValue(propNode, COLUMN_OID_UNASSIGNED));
                iterator.remove();
            } else if (propName.equals("dropped")) {
                builder.setDropped(nodeBooleanValue(propNode));
                iterator.remove();
            }
        }
    }

    private static IndexOptions nodeIndexOptionValue(final Object propNode) {
        final String value = propNode.toString();
        if (INDEX_OPTIONS_OFFSETS.equalsIgnoreCase(value)) {
            return IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
        } else if (INDEX_OPTIONS_POSITIONS.equalsIgnoreCase(value)) {
            return IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
        } else if (INDEX_OPTIONS_FREQS.equalsIgnoreCase(value)) {
            return IndexOptions.DOCS_AND_FREQS;
        } else if (INDEX_OPTIONS_DOCS.equalsIgnoreCase(value)) {
            return IndexOptions.DOCS;
        } else {
            throw new ElasticsearchParseException("failed to parse index option [{}]", value);
        }
    }

    public static void parseTermVector(String fieldName, String termVector, TextFieldMapper.Builder builder) throws MapperParsingException {
        if ("no".equals(termVector)) {
            builder.storeTermVectors(false);
        } else if ("yes".equals(termVector)) {
            builder.storeTermVectors(true);
        } else if ("with_offsets".equals(termVector)) {
            builder.storeTermVectorOffsets(true);
        } else if ("with_positions".equals(termVector)) {
            builder.storeTermVectorPositions(true);
        } else if ("with_positions_offsets".equals(termVector)) {
            builder.storeTermVectorPositions(true);
            builder.storeTermVectorOffsets(true);
        } else if ("with_positions_payloads".equals(termVector)) {
            builder.storeTermVectorPositions(true);
            builder.storeTermVectorPayloads(true);
        } else if ("with_positions_offsets_payloads".equals(termVector)) {
            builder.storeTermVectorPositions(true);
            builder.storeTermVectorOffsets(true);
            builder.storeTermVectorPayloads(true);
        } else {
            throw new MapperParsingException("wrong value for termVector [" + termVector + "] for field [" + fieldName + "]");
        }
    }

    public static void parseCopyFields(Object propNode, FieldMapper.Builder<?> builder) {
        FieldMapper.CopyTo.Builder copyToBuilder = new FieldMapper.CopyTo.Builder();
        if (propNode instanceof List<?> nodes) {
            for (Object node : nodes) {
                copyToBuilder.add(nodeStringValue(node, null));
            }
        } else {
            copyToBuilder.add(nodeStringValue(propNode, null));
        }
        builder.copyTo(copyToBuilder.build());
    }
}
