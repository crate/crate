/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.engine.collect.files;

import io.crate.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UriWithGlob {
    private static final Pattern HAS_GLOBS_PATTERN = Pattern.compile("^((s3://|file://|/)[^\\*]*/)[^\\*]*\\*.*");

    private final URI uri;
    private final URI preGlobUri;
    @Nullable
    final Predicate<URI> globPredicate;

    private UriWithGlob(URI uri, URI preGlobUri, Predicate<URI> globPredicate) {
        this.uri = uri;
        this.preGlobUri = preGlobUri;
        this.globPredicate = globPredicate;
    }

    @Nullable
    public static UriWithGlob toUriWithGlob(URI fileUri, @Nullable Function<String, URI> uriFormatter) {
        if (uriFormatter != null) {
            fileUri = uriFormatter.apply(fileUri.toString());
        } else {
            uriFormatter = FileReadingIterator::toURI;
        }
        String formattedUriStr = fileUri.toString();
        URI preGlobUri = null;
        Predicate<URI> globPredicate = null;
        Matcher hasGlobMatcher = HAS_GLOBS_PATTERN.matcher(formattedUriStr);
        /*
         * hasGlobMatcher.group(1) returns part of the path before the wildcards with a trailing backslash,
         * ex)
         *      'file:///bucket/prefix/*.json'                           -> 'file:///bucket/prefix/'
         *      's3://bucket/year=2020/month=12/day=*0/hour=12/*.json'   -> 's3://bucket/year=2020/month=12/'
         */
        if (hasGlobMatcher.matches()) {
            if (formattedUriStr.startsWith("/") || formattedUriStr.startsWith("file://")) {
                Path oldPath = Paths.get(uriFormatter.apply(hasGlobMatcher.group(1)));
                String oldPathAsString;
                String newPathAsString;
                try {
                    oldPathAsString = oldPath.toUri().toString();
                    newPathAsString = oldPath.toRealPath().toUri().toString();
                } catch (IOException e) {
                    return null;
                }
                //resolve any links
                String resolvedFileUrl = formattedUriStr.replace(oldPathAsString, newPathAsString);
                fileUri = uriFormatter.apply(resolvedFileUrl);
                preGlobUri = uriFormatter.apply(newPathAsString);
            } else {
                preGlobUri = URI.create(hasGlobMatcher.group(1));
            }
            globPredicate = new GlobPredicate(fileUri);
        }
        return new UriWithGlob(fileUri, preGlobUri, globPredicate);
    }

    public URI getUri() {
        return uri;
    }

    public URI getPreGlobUri() {
        return preGlobUri;
    }

    @Nullable
    public Predicate<URI> getGlobPredicate() {
        return globPredicate;
    }

    @VisibleForTesting
    static class GlobPredicate implements Predicate<URI> {
        private final Pattern globPattern;

        GlobPredicate(URI fileUri) {
            this.globPattern = Pattern.compile(Globs.toUnixRegexPattern(fileUri.toString()));
        }

        @Override
        public boolean test(@Nullable URI input) {
            return input != null && globPattern.matcher(input.toString()).matches();
        }
    }
}
