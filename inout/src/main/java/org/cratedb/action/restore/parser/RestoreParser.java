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

package org.cratedb.action.restore.parser;

import com.google.common.collect.ImmutableMap;
import org.cratedb.action.dump.parser.DumpParser;
import org.cratedb.action.import_.ImportContext;
import org.cratedb.action.import_.parser.PathParseElement;
import org.cratedb.action.import_.parser.IImportParser;
import org.cratedb.action.import_.parser.ImportParseElement;
import org.cratedb.action.import_.parser.ImportParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class RestoreParser implements IImportParser {

    private final ImmutableMap<String, ImportParseElement> elementParsers;

    public static final String FILE_PATTERN = ".*_.*_.*\\.json\\.gz";

    public RestoreParser() {
        Map<String, ImportParseElement> elementParsers = new HashMap<String, ImportParseElement>();
        elementParsers.put("path", new PathParseElement());
        // deprecated but still here for backward compatibility
        elementParsers.put("directory", new PathParseElement());
        this.elementParsers = ImmutableMap.copyOf(elementParsers);
    }

    /**
     * Main method of this class to parse given payload of _restore action
     *
     * @param context
     * @param source
     * @throws org.elasticsearch.search.SearchParseException
     */
    public void parseSource(ImportContext context, BytesReference source) throws ImportParseException {
        XContentParser parser = null;
        this.setDefaults(context);
        context.settings(true);
        context.mappings(true);
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
            if (context.path() == null) {
                context.path(DumpParser.DEFAULT_DIR);
            }
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
     * Set restore specific default values to the context like compression and file_pattern
     *
     * @param context
     */
    private void setDefaults(ImportContext context) {
        context.compression(true);
        Pattern p = Pattern.compile(FILE_PATTERN);
        context.file_pattern(p);
    }
}
