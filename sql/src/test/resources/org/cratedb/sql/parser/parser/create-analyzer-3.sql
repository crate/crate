create analyzer myAnalyzer extends otherAnalyzer with (
    token_filters with (
        "my_filter" with (
            bla='blubb'
        ),
        myOtherTokenFilter (
            "false"=true,
            one=1
        )
    )
)