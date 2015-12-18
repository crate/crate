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

import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.sys.SysMatthiasTableInfo;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class SysMatthiasExpressionModule extends AbstractModule {
    @Override
    protected void configure() {
        MapBinder<ReferenceIdent, ReferenceImplementation> b = MapBinder
                .newMapBinder(binder(), ReferenceIdent.class, ReferenceImplementation.class);
        b.addBinding(matthiasIdent(SysMatthiasTableInfo.NAME)).to(MatthiasNameExpression.class).asEagerSingleton();
        b.addBinding(matthiasIdent(SysMatthiasTableInfo.FROM)).to(MatthiasFromExpression.class).asEagerSingleton();
        b.addBinding(matthiasIdent(SysMatthiasTableInfo.TO)).to(MatthiasToExpression.class).asEagerSingleton();
        b.addBinding(matthiasIdent(SysMatthiasTableInfo.TITLE)).to(MatthiasTitleExpression.class).asEagerSingleton();
        b.addBinding(matthiasIdent(SysMatthiasTableInfo.STATS)).to(MatthiasStatsExpression.class).asEagerSingleton();
        b.addBinding(matthiasIdent(SysMatthiasTableInfo.FAVORITE_PROGRAMMING_LANGUAGE)).to(MatthiasFavoritProgrammingLanguageExpression.class).asEagerSingleton();
        b.addBinding(matthiasIdent(SysMatthiasTableInfo.MESSAGE)).to(MatthiasMessageExpression.class).asEagerSingleton();
        b.addBinding(matthiasIdent(SysMatthiasTableInfo.GIFS)).to(MatthiasGifsExpression.class).asEagerSingleton();
    }

    private ReferenceIdent matthiasIdent(String name) {
        return new ReferenceIdent(SysMatthiasTableInfo.IDENT, name);
    }
}
