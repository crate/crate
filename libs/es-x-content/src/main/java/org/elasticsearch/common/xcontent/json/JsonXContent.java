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

package org.elasticsearch.common.xcontent.json;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.jetbrains.annotations.Nullable;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.StreamWriteFeature;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.core.json.JsonWriteFeature;

/**
 * A JSON based content implementation using Jackson.
 */
public class JsonXContent implements XContent {

    private static final JsonFactory JSON_FACTORY = new JsonFactoryBuilder()
        .configure(JsonWriteFeature.QUOTE_FIELD_NAMES, true)
        .configure(JsonReadFeature.ALLOW_JAVA_COMMENTS, true)
        // this trips on many mappings now...
        .configure(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false)
        // Do not automatically close unclosed objects/arrays in com.fasterxml.jackson.core.json.UTF8JsonGenerator#close() method
        .configure(StreamWriteFeature.AUTO_CLOSE_CONTENT, false)
        .configure(StreamReadFeature.STRICT_DUPLICATE_DETECTION, XContent.isStrictDuplicateDetectionEnabled())
        .build();
    public static final JsonXContent JSON_XCONTENT = new JsonXContent();

    public static XContentBuilder builder() throws IOException {
        return XContentBuilder.builder(JSON_XCONTENT);
    }

    private JsonXContent() {
    }

    @Override
    public XContentType type() {
        return XContentType.JSON;
    }

    @Override
    public byte streamSeparator() {
        return '\n';
    }

    @Override
    public XContentGenerator createGenerator(OutputStream os, @Nullable String rootValueSeparator) throws IOException {
        JsonGenerator generator = JSON_FACTORY.createGenerator(os, JsonEncoding.UTF8);
        if (rootValueSeparator != null) {
            generator.setRootValueSeparator(new SerializedString(rootValueSeparator));
        }
        return new JsonXContentGenerator(generator, os);
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, String content) throws IOException {
        return new JsonXContentParser(xContentRegistry, deprecationHandler, JSON_FACTORY.createParser(new StringReader(content)));
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, InputStream is) throws IOException {
        return new JsonXContentParser(xContentRegistry, deprecationHandler, JSON_FACTORY.createParser(is));
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, byte[] data) throws IOException {
        return new JsonXContentParser(xContentRegistry, deprecationHandler, JSON_FACTORY.createParser(data));
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, byte[] data, int offset, int length) throws IOException {
        return new JsonXContentParser(xContentRegistry, deprecationHandler, JSON_FACTORY.createParser(data, offset, length));
    }
}
