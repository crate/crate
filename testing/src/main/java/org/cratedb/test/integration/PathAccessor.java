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

package org.cratedb.test.integration;


import com.google.common.base.Charsets;
import org.elasticsearch.common.io.Streams;

import java.io.*;

public class PathAccessor {

    public static String stringFromPath(String path, Class<?> aClass) throws IOException {
        return Streams.copyToString(new InputStreamReader(
                getInputStream(path, aClass),
                Charsets.UTF_8));
    }

    public static byte[] bytesFromPath(String path, Class<?> aClass) throws IOException {
        InputStream is = getInputStream(path, aClass);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Streams.copy(is, out);
        is.close();
        out.close();
        return out.toByteArray();
    }

    public static InputStream getInputStream(String path, Class<?> aClass) throws FileNotFoundException {
        InputStream is = aClass.getResourceAsStream(path);
        if (is == null) {
            throw new FileNotFoundException("Resource [" + path + "] not found in classpath");
        }
        return is;
    }
}
