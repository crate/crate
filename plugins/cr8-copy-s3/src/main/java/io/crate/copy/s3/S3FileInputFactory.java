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

package io.crate.copy.s3;

import java.net.URI;

import org.apache.opendal.AsyncExecutor;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Operator;
import org.apache.opendal.ServiceConfig.S3;
import org.elasticsearch.common.settings.Settings;

import io.crate.copy.s3.common.S3Env;
import io.crate.copy.s3.common.S3URI;
import io.crate.execution.engine.collect.files.FileInputFactory;
import io.crate.execution.engine.collect.files.Globs;
import io.crate.opendal.OpenDALFileInput;

public class S3FileInputFactory implements FileInputFactory {

    public static final String NAME = "s3";
    private final AsyncExecutor executor;

    public S3FileInputFactory(AsyncExecutor executor) {
        this.executor = executor;
    }

    @Override
    public OpenDALFileInput create(URI uri, Settings withClause) {
        S3URI s3uri = S3URI.of(uri);
        S3 s3 = S3Env.getServiceConfig(s3uri, withClause);
        Operator operator = AsyncOperator.of(s3, executor).blocking();
        StringBuilder newURI = new StringBuilder();
        newURI
            .append(s3.scheme())
            .append("://")
            .append(s3uri.bucket())
            .append("/");
        String path = s3uri.path();
        int globIdx = path.indexOf("*");
        String preGlobPath = null;
        Globs.GlobPredicate globPredicate = null;
        if (globIdx == -1) {
            newURI.append(path);
        } else {
            preGlobPath = path.substring(0, globIdx);
            globPredicate = new Globs.GlobPredicate(path);
        }
        return new OpenDALFileInput(
            operator,
            URI.create(newURI.toString()),
            preGlobPath,
            globPredicate
        );
    }
}
