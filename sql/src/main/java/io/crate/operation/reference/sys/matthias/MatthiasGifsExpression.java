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

package io.crate.operation.reference.sys.matthias;

import io.crate.metadata.SimpleObjectExpression;
import org.apache.lucene.util.BytesRef;

public class MatthiasGifsExpression extends SimpleObjectExpression<BytesRef[]> {

    public static final BytesRef[] GIFS = new BytesRef[] {
            new BytesRef("http://tclhost.com/6E71Pl6.gif"),
            new BytesRef("http://33.media.tumblr.com/e8091211fe0e60230acd9e1a1e1e2ec9/tumblr_inline_ny8tujouJG1raprkq_500.gif"),
            new BytesRef("https://media.giphy.com/media/10QbzBy3KKAw36/giphy.gif"),
            new BytesRef("http://www.reactiongifs.com/r/spg.gif"),
            new BytesRef("http://www.reactiongifs.com/wp-content/uploads/2013/01/lol-bye.gif"),
            new BytesRef("http://www.reactiongifs.com/r/best-gif-ever1.gif")
    };

    @Override
    public BytesRef[] value() {
        return GIFS;
    }
}
