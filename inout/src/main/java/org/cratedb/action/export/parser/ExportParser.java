/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.action.export.parser;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FieldsParseElement;
import org.elasticsearch.search.fetch.explain.ExplainParseElement;
import org.elasticsearch.search.query.QueryPhase;

import org.cratedb.action.export.ExportContext;

/**
 * Parser for payload given to _export action.
 */
public class ExportParser implements IExportParser {

    private final ImmutableMap<String, SearchParseElement> elementParsers;

    @Inject
    public ExportParser(QueryPhase queryPhase, FetchPhase fetchPhase) {
        Map<String, SearchParseElement> elementParsers = new HashMap<String, SearchParseElement>();
        elementParsers.putAll(queryPhase.parseElements());
        elementParsers.put("fields", new FieldsParseElement());
        elementParsers.put("output_cmd", new ExportOutputCmdParseElement());
        elementParsers.put("output_file", new ExportOutputFileParseElement());
        elementParsers.put("force_overwrite", new ExportForceOverwriteParseElement());
        elementParsers.put("compression", new ExportCompressionParseElement());
        elementParsers.put("explain", new ExplainParseElement());
        elementParsers.put("mappings", new ExportMappingsParseElement());
        elementParsers.put("settings", new ExportSettingsParseElement());
        this.elementParsers = ImmutableMap.copyOf(elementParsers);
    }

    /**
     * validate given payload
     *
     * @param context
     */
    private void validate(ExportContext context) {
        if (!context.hasFieldNames()) {
            throw new SearchParseException(context, "No export fields defined");
        }
        for (String field : context.fieldNames()) {
            if (context.mapperService().name(field) == null && !field.equals("_version")) {
                throw new SearchParseException(context, "Export field [" + field + "] does not exist in the mapping");
            }
        }
        if (context.outputFile() != null) {
            if (context.outputCmdArray() != null || context.outputCmd() != null) {
                throw new SearchParseException(context, "Concurrent definition of 'output_cmd' and 'output_file'");
            }
        } else if (context.outputCmdArray() == null && context.outputCmd() == null) {
            throw new SearchParseException(context, "'output_cmd' or 'output_file' has not been defined");
        } else if (context.outputFile() == null && context.settings()) {
            throw new SearchParseException(context, "Parameter 'settings' requires usage of 'output_file'");
        } else if (context.outputFile() == null && context.mappings()) {
            throw new SearchParseException(context, "Parameter 'mappings' requires usage of 'output_file'");
        }
    }

    /**
     * Main method of this class to parse given payload of _export action
     *
     * @param context
     * @param source
     * @throws SearchParseException
     */
    public void parseSource(ExportContext context, BytesReference source) throws SearchParseException {
        XContentParser parser = null;
        try {
            if (source != null && source.length() != 0) {
                parser = XContentFactory.xContent(source).createParser(source);
                XContentParser.Token token;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        String fieldName = parser.currentName();
                        parser.nextToken();
                        SearchParseElement element = elementParsers.get(fieldName);
                        if (element == null) {
                            throw new SearchParseException(context, "No parser for element [" + fieldName + "]");
                        }
                        element.parse(parser, context);
                    } else if (token == null) {
                        break;
                    }
                }
            }
            validate(context);
        } catch (Exception e) {
            String sSource = "_na_";
            try {
                sSource = XContentHelper.convertToJson(source, false);
            } catch (Throwable e1) {
                // ignore
            }
            throw new SearchParseException(context, "Failed to parse source [" + sSource + "]", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }
}