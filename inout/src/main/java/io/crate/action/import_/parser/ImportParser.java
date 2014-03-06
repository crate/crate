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

package io.crate.action.import_.parser;

import com.google.common.collect.ImmutableMap;
import io.crate.action.import_.ImportContext;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;

import java.util.HashMap;
import java.util.Map;

public class ImportParser implements IImportParser {

    private final ImmutableMap<String, ImportParseElement> elementParsers;

    public ImportParser() {
        Map<String, ImportParseElement> elementParsers = new HashMap<String, ImportParseElement>();
        elementParsers.put("path", new PathParseElement());
        elementParsers.put("directory", new PathParseElement());
        elementParsers.put("compression", new ImportCompressionParseElement());
        elementParsers.put("file_pattern", new FilePatternParseElement());
        elementParsers.put("mappings", new ImportMappingsParseElement());
        elementParsers.put("settings", new ImportSettingsParseElement());
        this.elementParsers = ImmutableMap.copyOf(elementParsers);
    }

    /**
     * Main method of this class to parse given payload of _export action
     *
     * @param context
     * @param source
     * @throws SearchParseException
     */
    public void parseSource(ImportContext context, BytesReference source) throws ImportParseException {
        XContentParser parser = null;
        try {
            if (source != null && source.length() != 0) {
                parser = XContentFactory.xContent(source).createParser(source);
                XContentParser.Token token;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        String fieldName = parser.currentName();
                        parser.nextToken();
                        ImportParseElement element = elementParsers.get(fieldName);
                        if (element == null) {
                            throw new ImportParseException(context, "No parser for element [" + fieldName + "]");
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
            throw new ImportParseException(context, "Failed to parse source [" + sSource + "]", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    /**
     * validate given payload
     *
     * @param context
     */
    private void validate(ImportContext context) {
        if (context.path() == null || context.path().isEmpty()) {
            throw new ImportParseException(context, "No path or directory defined");
        }
    }
}
