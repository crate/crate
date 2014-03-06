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

package io.crate.action.dump.parser;

import io.crate.action.export.ExportContext;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

import java.io.File;


/**
 * Parser element class to parse a given 'directory' option to the _dump endpoint
 */
public class DumpDirectoryParseElement implements SearchParseElement {


    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token.isValue()) {
            setOutPutFile((ExportContext) context, parser.text());
        }
    }

    /**
     * Set the constant filename_pattern prefixed with a target directory as output_file to the context
     *
     * @param context
     * @param directory
     */
    public void setOutPutFile(ExportContext context, String directory) {
        File dir = new File(directory);
        File file = new File(dir, DumpParser.FILENAME_PATTERN);
        context.outputFile(file.getPath());
    }

}
