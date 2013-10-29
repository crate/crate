create analyzer a4 extends a3 with (
    token_filters with (
        mytokenfilter,
        blubb with (
            stopwords='bla, blubb'
        )
    ),
    tokenizer alphabet with (
        a='b',
        c='d'
    ),
    char_filters with (
        html_strip
    )

)