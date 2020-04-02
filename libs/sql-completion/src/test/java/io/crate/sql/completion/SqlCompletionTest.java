package io.crate.sql.completion;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SqlCompletionTest {

    private SqlCompletion completion;

    @BeforeEach
    public void setupCompletion() {
        this.completion = new SqlCompletion();
    }

    @Test
    public void test_complete_sel_to_select() throws Exception {
        var candidates = completion.getCandidates("SEL");
        assertThat(candidates, Matchers.contains(is("SELECT")));
    }

    @Test
    public void test_complete_se() throws Exception {
        var candidates = completion.getCandidates("SE");
        assertThat(candidates, Matchers.contains(
            is("SELECT"),
            is("SET")
        ));
    }

    @Test
    public void test_complete_up() throws Exception {
        var candidates = completion.getCandidates("UP");
        assertThat(candidates, Matchers.contains(is("UPDATE")));
    }

    @Test
    public void test_complete_de() throws Exception {
        var candidates = completion.getCandidates("DE");
        assertThat(candidates, Matchers.contains(
            is("DEALLOCATE"),
            is("DELETE"),
            is("DENY")
        ));
    }

    @Test
    public void test_complete_re() throws Exception {
        var candidates = completion.getCandidates("RE");
        assertThat(candidates, Matchers.contains(
            is("REFRESH"),
            is("RESTORE"),
            is("RESET"),
            is("REVOKE")
        ));
    }

    @Test
    public void test_complete_cr() throws Exception {
        var candidates = completion.getCandidates("CR");
        assertThat(candidates, Matchers.contains(is("CREATE")));
    }
}
