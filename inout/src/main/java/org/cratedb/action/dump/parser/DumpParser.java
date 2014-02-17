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

package org.cratedb.action.dump.parser;

import com.google.common.collect.ImmutableMap;
import org.cratedb.action.export.ExportContext;
import org.cratedb.action.export.parser.ExportForceOverwriteParseElement;
import org.cratedb.action.export.parser.IExportParser;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.query.QueryPhase;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Dump specific parser class
 */
public class DumpParser implements IExportParser {

    public static final String[] DEFAULT_FIELDS = {"_id", "_source", "_timestamp", "_ttl", "_version", "_index",
                                                   "_type", "_routing"};
    public static final String FILENAME_PATTERN = "${cluster}_${index}_${shard}.json.gz";
    public static final String DEFAULT_DIR = "dump";


    private final ImmutableMap<String, SearchParseElement> elementParsers;
    private final DumpDirectoryParseElement directoryParseElement = new DumpDirectoryParseElement();


    @Inject
    public DumpParser(QueryPhase queryPhase, FetchPhase fetchPhase) {
        Map<String, SearchParseElement> elementParsers = new HashMap<String, SearchParseElement>();
        elementParsers.putAll(queryPhase.parseElements());
        elementParsers.put("force_overwrite", new ExportForceOverwriteParseElement());
        elementParsers.put("directory", directoryParseElement);
        this.elementParsers = ImmutableMap.copyOf(elementParsers);
    }

    /**
     * Main method of this class to parse given payload of _dump action
     *
     * @param context
     * @param source
     * @throws SearchParseException
     */
    public void parseSource(ExportContext context, BytesReference source) throws SearchParseException {
        XContentParser parser = null;
        this.setDefaults(context);
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
            if (context.outputFile() == null) {
                directoryParseElement.setOutPutFile(context, DEFAULT_DIR);
                this.ensureDefaultDirectory(context);
            }
            context.mappings(true);
            context.settings(true);
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

    /**
     * Set dump specific default values to the context like directory, compression or fields to export
     *
     * @param context
     */
    private void setDefaults(ExportContext context) {
        context.compression(true);
        for (int i = 0; i < DEFAULT_FIELDS.length; i++) {
            context.fieldNames().add(DEFAULT_FIELDS[i]);
        }
    }

    /**
     * create default dump directory if it does not exist
     *
     * @param context
     */
    private void ensureDefaultDirectory(ExportContext context) {
        File dumpFile = new File(context.outputFile());
        File dumpDir = new File(dumpFile.getParent());
        if (!dumpDir.exists()) {
            dumpDir.mkdir();
        }
    }
}
