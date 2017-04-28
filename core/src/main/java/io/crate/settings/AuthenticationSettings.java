package io.crate.settings;


import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Setting;

public class AuthenticationSettings {

    public static final CrateSetting AUTH_HOST_BASED_SETTING = CrateSetting.of(
        Setting.groupSetting("auth.host_based.",
            Setting.Property.Dynamic,
            Setting.Property.NodeScope),
        DataTypes.OBJECT
    );
}
