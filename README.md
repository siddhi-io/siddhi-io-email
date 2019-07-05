Siddhi IO Email
======================================

  [![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-email/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-email/)
  [![GitHub (pre-)Release](https://img.shields.io/github/release/siddhi-io/siddhi-io-email/all.svg)](https://github.com/siddhi-io/siddhi-io-email/releases)
  [![GitHub (Pre-)Release Date](https://img.shields.io/github/release-date-pre/siddhi-io/siddhi-io-email.svg)](https://github.com/siddhi-io/siddhi-io-email/releases)
  [![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-io-email.svg)](https://github.com/siddhi-io/siddhi-io-email/issues)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-io-email.svg)](https://github.com/siddhi-io/siddhi-io-email/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-io-email extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that receives and publishes events via EMAIL and HTTPS transports, calls external services, and serves incoming requests and provide synchronous responses.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Download

* Versions 2.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.io.email/siddhi-io-email/">here</a>.
* Versions 1.x and lower with group id `org.wso2.extension.siddhi.*` from <a target="_blank" href="https://mvnrepository.com/artifact/org.wso2.extension.siddhi.io.email/siddhi-io-email">here</a>.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-email/api/2.0.2">2.0.2</a>.

## Features

* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-email/api/2.0.2/#email-sink">email</a> *<a target="_blank" href="email://siddhi.io/documentation/siddhi-5.x/query-guide-5.x/#sink">(Sink)</a>*<br><div style="padding-left: 1em;"><p>The email sink uses the 'smtp' server to publish events via emails. The events can be published in 'text', 'xml' or 'json' formats. The user can define email sink parameters in either the '&lt;SP_HOME&gt;/conf/&lt;PROFILE&gt;/deployment yaml' file or in the stream definition. The email sink first checks the stream definition for parameters, and if they are no configured there, it checks the 'deployment.yaml' file. If the parameters are not configured in either place, default values are considered for optional parameters. If you need to configure server system parameters that are not provided as options in the stream definition, then those parameters need to be defined them in the 'deployment.yaml' file under 'email sink properties'.<br><br>For more information about the SMTP server parameters, see  https://javaee.github.io/javamail/SMTP-Transport.<br><br>Further, some email accounts are required to enable the 'access to less secure apps' option. For gmail accounts, you can enable this option via https://myaccount.google.com/lesssecureapps.</p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-email/api/2.0.2/#email-source">email</a> *<a target="_blank" href="email://siddhi.io/documentation/siddhi-5.x/query-guide-5.x/#source">(Source)</a>*<br><div style="padding-left: 1em;"><p>The 'Email' source allows you to receive events via emails. An 'Email' source can be configured using the 'imap' or 'pop3' server to receive events. This allows you to filter the messages that satisfy the criteria specified under the 'search term' option. The email source parameters can be defined in either the '&lt;SP_HOME&gt;/conf/&lt;PROFILE&gt;/deployment yaml' file or the stream definition. If the parameter configurations are not available in either place, the default values are considered (i.e., if default values are available). If you need to configure server system parameters that are not provided as options in the stream definition, they need to be defined in the 'deployment yaml' file under 'email source properties'. For more information about 'imap' and 'pop3' server system parameters, see the following.<br>[JavaMail Reference Implementation - IMAP Store](https://javaee.github.io/javamail/IMAP-Store)<br>[JavaMail Reference Implementation - POP3 Store Store](https://javaee.github.io/javamail/POP3-Store)</p></div>

## Dependencies 

There are no other dependencies needed for this extension. 

## Installation

For installing this extension on various siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.

## Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-string/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.
