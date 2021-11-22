/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.collect.files;

import io.crate.common.collections.Lists2;
import io.crate.execution.engine.collect.files.FileReadingIterator.GlobPredicate;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class LocalFsFileInputTest extends ESTestCase {

    private List<String> helper_ListUris(String originalUri) throws Exception {
        String preGlob = originalUri.substring(0, originalUri.indexOf('*'));
        URI preGlobeUri = new URI(preGlob);
        URI uri = URI.create(originalUri);
        Predicate<URI> uriPredicate = new GlobPredicate(uri);
        return Lists2.map(new LocalFsFileInput().listUris(uri, preGlobeUri, uriPredicate, null), URI::toString);
    }

    @Test
    public void test_listUris_with_fileUri_containing_links() throws Exception {
        final String PREFIX = Paths.get(getClass().getResource("/essetup/data/").toURI()).toUri().toString();
        Files.deleteIfExists(Path.of(new URI(PREFIX + "linked/a/b/c/symlink_to_a")));
        Files.deleteIfExists(Path.of(new URI(PREFIX + "linked/a/b/c/symlink_to_e")));
        Files.createSymbolicLink(
            Path.of(new URI(PREFIX + "linked/a/b/c/symlink_to_a")), Path.of(new URI(PREFIX + "linked/a")));
        Files.createSymbolicLink(
            Path.of(new URI(PREFIX + "linked/a/b/c/symlink_to_e")), Path.of(new URI(PREFIX + "linked/a/b/c/d/e")));

        Map<String, List<String>> userInputToExpectedUris = Map.ofEntries(
            Map.entry(PREFIX + "linked/*/*.json",
                      List.of(PREFIX + "linked/a/a.json")
            ),
            Map.entry(PREFIX + "linked/*/*/*/*.json",
                      List.of(PREFIX + "linked/a/b/c/c.json")
            ),
            Map.entry(PREFIX + "linked/*/*/*/*/*.json",
                      List.of(PREFIX + "linked/a/b/c/d/hardlink_to_b.json",
                              PREFIX + "linked/a/b/c/symlink_to_a/a.json",
                              PREFIX + "linked/a/b/c/symlink_to_e/e.json")
            ),
            Map.entry(PREFIX + "linked/a/b/c/*/*/*/*/*/*.json",
                      List.of(PREFIX + "linked/a/b/c/symlink_to_a/b/c/d/e/e.json",
                              PREFIX + "linked/a/b/c/symlink_to_a/b/c/symlink_to_a/b/b.json",
                              PREFIX + "linked/a/b/c/symlink_to_a/b/c/symlink_to_e/f/f.json")
            ),
            Map.entry(PREFIX + "*/*/*/*/*/*/*/*/*/*.json",
                      List.of(PREFIX + "linked/a/b/c/symlink_to_a/b/c/d/e/e.json",
                              PREFIX + "linked/a/b/c/symlink_to_a/b/c/symlink_to_a/b/b.json",
                              PREFIX + "linked/a/b/c/symlink_to_a/b/c/symlink_to_e/f/f.json")
            ),
            Map.entry(PREFIX + "*/*/*/*/*/*/*/*/*/f.json",
                      List.of(PREFIX + "linked/a/b/c/symlink_to_a/b/c/symlink_to_e/f/f.json")
            )
        );
        for (var e : userInputToExpectedUris.entrySet()) {
            var actual = helper_ListUris(e.getKey()).stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
            var expected = e.getValue().stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
            assertEquals(expected, actual);
        }
    }
}
