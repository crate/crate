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

package org.elasticsearch.common.xcontent;

import java.io.IOException;
import java.io.OutputStream;

import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.smile.SmileXContent;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.jetbrains.annotations.Nullable;

/**
 * A one stop to use {@link org.elasticsearch.common.xcontent.XContent} and {@link XContentBuilder}.
 */
public class XContentFactory {

    /**
     * Constructs a new json builder that will output the result into the provided output stream.
     */
    public static XContentBuilder json(OutputStream os) throws IOException {
        return json(os, null);
    }

    /**
     * Constructs a new json builder that will output the result into the provided output stream.
     * @param os the output stream
     * @param rootValueSeparator the rootValueSeparator, if null the default whitespace (" ") is used
     */
    public static XContentBuilder json(OutputStream os, @Nullable String rootValueSeparator) throws IOException {
        return new XContentBuilder(JsonXContent.JSON_XCONTENT, os, rootValueSeparator);
    }

    /**
     * Constructs a new json builder that will output the result into the provided output stream.
     */
    public static XContentBuilder smile(OutputStream os) throws IOException {
        return new XContentBuilder(SmileXContent.SMILE_XCONTENT, os);
    }

    /**
     * Constructs a new yaml builder that will output the result into the provided output stream.
     */
    public static XContentBuilder yaml(OutputStream os) throws IOException {
        return new XContentBuilder(YamlXContent.YAML_XCONTENT, os);
    }

    /**
     * Constructs a xcontent builder that will output the result into the provided output stream.
     */
    public static XContentBuilder builder(XContentType type, OutputStream outputStream) throws IOException {
        return switch (type) {
            case JSON -> new XContentBuilder(JsonXContent.JSON_XCONTENT, outputStream);
            case SMILE -> new XContentBuilder(SmileXContent.SMILE_XCONTENT, outputStream);
            case YAML -> new XContentBuilder(YamlXContent.YAML_XCONTENT, outputStream);
        };
    }
}
