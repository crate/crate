package io.crate.role;

import static io.crate.role.Role.CRATE_USER;
import static io.crate.role.metadata.RolesHelper.DUMMY_USERS_AND_ROLES;
import static io.crate.role.metadata.RolesHelper.roleOf;
import static io.crate.role.metadata.RolesHelper.userOf;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Map;

import org.junit.Test;

import io.crate.role.metadata.RolesMetadata;
import io.crate.role.metadata.UsersMetadata;
import io.crate.role.metadata.UsersPrivilegesMetadata;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class RolesServiceTest extends CrateDummyClusterServiceUnitTest {

    private static final Map<String, Role> DEFAULT_USERS = Map.of(CRATE_USER.name(), CRATE_USER);

    @Test
    public void testNullAndEmptyMetadata() {
        // the users list will always contain a crate user
        Map<String, Role> roles = RolesService.getRoles(null, null, null);
        assertThat(roles).containsExactlyEntriesOf(DEFAULT_USERS);

        roles = RolesService.getRoles(new UsersMetadata(), new RolesMetadata(), new UsersPrivilegesMetadata());
        assertThat(roles).containsExactlyEntriesOf(DEFAULT_USERS);
    }

    @Test
    public void testUsersAndRoles() {
        Map<String, Role> roles = RolesService.getRoles(
            null,
            new RolesMetadata(DUMMY_USERS_AND_ROLES),
            new UsersPrivilegesMetadata());

        assertThat(roles).containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "Ford", DUMMY_USERS_AND_ROLES.get("Ford"),
                "John", DUMMY_USERS_AND_ROLES.get("John"),
                "DummyRole", roleOf("DummyRole"),
                CRATE_USER.name(), CRATE_USER));
    }

    @Test
    public void test_old_users_metadata_is_preferred_over_roles_metadata() {
        Map<String, Role> roles = RolesService.getRoles(
            new UsersMetadata(Collections.singletonMap("Arthur", null)),
            new RolesMetadata(DUMMY_USERS_AND_ROLES),
            new UsersPrivilegesMetadata());

        assertThat(roles).containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "Arthur" , userOf("Arthur"),
                CRATE_USER.name(), CRATE_USER));
    }
}
