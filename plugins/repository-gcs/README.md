Google Cloud Storage Plugin
===========================
This plugin was developed by Herman Bergwerf ([1]) on behalf of Profects B.V.
([2]). This document is intended to explain some details and keep a list of
attention points. The code for this plugin is based on the Azure plugin, and
uses the Google Cloud Storage Java library that is documented at ([3]).

Authentication
--------------
In principle there is no need for this plugin to handle authentication. Inside
the Google Ecosystem the project ID and credentials can be resolved
automatically (this is called [Application Default Credentials][4]), and outside
of it you can supply these with environment variables.

### Application Default Credentials
When using Google Cloud libraries from a Google Cloud Platform environment such
as Compute Engine, Kubernetes Engine, or App Engine, no additional
authentication steps are necessary ([5]).

### Environment Variables
Outside the Google Ecosystem it is necessary to provide a project ID and
credentials to the client library. It is recommended to use a service account
for authentication ([6]). The following environment variables must be set:

+ Set `GOOGLE_CLOUD_PROJECT` to the project ID ([7]).
+ Set `GOOGLE_APPLICATION_CREDENTIALS` to the service account JSON key.

These values can also be configured via Java code, but we prefer to avoid this
since it requires additional plugin settings.

What files are involved?
------------------------
Apart from the files in this folder, the plugin also involves the following
files:

+ `3RD-PARTY-NOTICES.md` (TODO)
+ `settings.gradle`
+ `app/build.gradle`
+ `gradle/version.properties`

[1]: https://hbergwerf.nl
[2]: https://profects.com
[3]: https://cloud.google.com/java/docs/reference/google-cloud-storage/latest/overview
[4]: https://cloud.google.com/docs/authentication/application-default-credentials
[5]: https://github.com/googleapis/google-cloud-java#google-cloud-platform-environment
[6]: https://github.com/googleapis/google-cloud-java#using-a-service-account-recommended
[7]: https://github.com/googleapis/google-cloud-java#specifying-a-project-id
