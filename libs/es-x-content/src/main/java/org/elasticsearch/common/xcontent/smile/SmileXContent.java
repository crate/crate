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

package org.elasticsearch.common.xcontent.smile;

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
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.StreamWriteFeature;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;

/**
 * A Smile based content implementation using Jackson.
 */
public class SmileXContent implements XContent {

    public static XContentBuilder contentBuilder() throws IOException {
        return XContentBuilder.builder(SMILE_XCONTENT);
    }

    static final SmileFactory SMILE_FACTORY = SmileFactory.builder()
        // for now, this is an overhead, might make sense for web sockets
        .configure(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT, false)
        .configure(SmileFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false) // this trips on many mappings now...
        // Do not automatically close unclosed objects/arrays in com.fasterxml.jackson.dataformat.smile.SmileGenerator#close() method
        .configure(StreamWriteFeature.AUTO_CLOSE_CONTENT, false)
        .configure(StreamReadFeature.STRICT_DUPLICATE_DETECTION, XContent.isStrictDuplicateDetectionEnabled())
        .build();
    public static final SmileXContent SMILE_XCONTENT = new SmileXContent();


    private SmileXContent() {
    }

    @Override
    public XContentType type() {
        return XContentType.SMILE;
    }

    @Override
    public byte streamSeparator() {
        return (byte) 0xFF;
    }

    @Override
    public XContentGenerator createGenerator(OutputStream os, @Nullable String rootValueSeparator) throws IOException {
        return new SmileXContentGenerator(SMILE_FACTORY.createGenerator(os, JsonEncoding.UTF8), os);
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, String content) throws IOException {
        return new SmileXContentParser(xContentRegistry, deprecationHandler, SMILE_FACTORY.createParser(new StringReader(content)));
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, InputStream is) throws IOException {
        return new SmileXContentParser(xContentRegistry, deprecationHandler, SMILE_FACTORY.createParser(is));
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, byte[] data) throws IOException {
        return new SmileXContentParser(xContentRegistry, deprecationHandler, SMILE_FACTORY.createParser(data));
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, byte[] data, int offset, int length) throws IOException {
        return new SmileXContentParser(xContentRegistry, deprecationHandler, SMILE_FACTORY.createParser(data, offset, length));
    }
}
