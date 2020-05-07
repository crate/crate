package org.elasticsearch.repositories.hdfs;

import java.util.List;
import java.util.Map;

import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.repositories.RepositoryParamValidator;
import io.crate.analyze.repositories.TypeSettings;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.GenericProperty;
import io.crate.sql.tree.StringLiteral;
import io.crate.test.integration.CrateUnitTest;

public class HdfsRepositoryAnalyzerTest extends CrateUnitTest {

    private RepositoryParamValidator validator;

    @Before
    public void prepare() {
        validator = new RepositoryParamValidator(
            Map.of("hdfs", new TypeSettings(List.of(), List.of()))
        );
    }

    @Test
    public void testValidateHdfsDynamicConfParam() throws Exception {
        GenericProperties<Expression> genericProperties = new GenericProperties<>();
        genericProperties.add(new GenericProperty<>("path", new StringLiteral("/data")));
        genericProperties.add(new GenericProperty<>("conf.foobar", new StringLiteral("bar")));
        validator.validate("hdfs", genericProperties, toSettings(genericProperties));
    }

    @Test
    public void testValidateHdfsSecurityPrincipal() throws Exception {
        GenericProperties<Expression> genericProperties = new GenericProperties<>();
        genericProperties.add(new GenericProperty<>("uri", new StringLiteral("hdfs://ha-name:8020")));
        genericProperties.add(new GenericProperty<>("security.principal", new StringLiteral("myuserid@REALM.DOMAIN")));
        genericProperties.add(new GenericProperty<>("path", new StringLiteral("/user/name/data")));
        genericProperties.add(new GenericProperty<>("conf.foobar", new StringLiteral("bar")));
        validator.validate("hdfs", genericProperties, toSettings(genericProperties));
    }

    private static Settings toSettings(GenericProperties<Expression> genericProperties) {
        Settings.Builder builder = Settings.builder();
        for (Map.Entry<String, Expression> property : genericProperties.properties().entrySet()) {
            builder.put(property.getKey(), property.getValue().toString());
        }
        return builder.build();
    }
}
