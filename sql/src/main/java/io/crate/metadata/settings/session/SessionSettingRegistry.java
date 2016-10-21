package io.crate.metadata.settings.session;

import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;

import java.util.Map;

public class SessionSettingRegistry {

    private static final Map<String, SessionSettingApplier> SESSION_SETTINGS =
        ImmutableMap.<String, SessionSettingApplier>builder()
            .put("search_path", new SessionSettingApplier() {

                @Override
                public void apply(String value, SessionContext context) {
                    if (value != null) {
                        String schemas[] = value.split("\\s*,\\s*");
                        assert schemas.length > 0 : "at least one schema should be provided";
                        context.setDefaultSchema(schemas[0]);
                    } else {
                        context.setDefaultSchema(null);
                    }
                }
            }).build();

    public static SessionSettingApplier getApplier(String setting) {
        return SESSION_SETTINGS.get(setting);
    }
}
