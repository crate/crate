# Security Issues

The CrateDB team and community take security bugs seriously. We appreciate your
efforts to [responsibly disclose] your findings, and will make every effort to
acknowledge your contributions.

## Reporting Security Issues

To report a security issue, please use the GitHub security reporting system on
the corresponding project where you discovered the flaw. It is on the
"Security" tab at Security » Advisories » New draft security advisory.

For CrateDB, just navigate to [Report a vulnerability for CrateDB].

Please do not report denial of service issues for _authenticated_ users as
security issues, use the [public issue tracker] instead for those. In a
database system it is generally possible for a single user to create a lot of
load with little effort, therefore we don't consider Denial of Service issues
critical except if they're possible for unauthenticated users.

The CrateDB team will send a response indicating the next steps in handling
your report. After the initial reply to your report, the security team will
keep you informed of the progress towards a fix and full announcement, and may
ask for additional information or guidance.


## Alternative Communication

If you can't use the GitHub security reporting system, an alternative way of
reporting security issues will be to write an email to security@crate.io.


[Report a vulnerability for CrateDB]: https://github.com/crate/crate/security/advisories/new
[responsibly disclose]: https://en.wikipedia.org/wiki/Coordinated_vulnerability_disclosure
[public issue tracker]: https://github.com/crate/crate/issues/
