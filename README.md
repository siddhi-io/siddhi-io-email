siddhi-io-email
======================================

The **siddhi-io-email extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that receives and publishes events via email.
Using the extension, events can be published through smtp mail server and received through 'pop3' or 'imap' mail serves.

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-email">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-email/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-email/issues">Issue tracker</a>

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-email/api/1.1.0">1.1.0</a>.

## How to use 

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support. 

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this extension you can replace the component <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-email/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.io.email</groupId>
        <artifactId>siddhi-io-email</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-email/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-email/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-email/api/1.1.0/#email-sink">email</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink">(Sink)</a>*<br><div style="padding-left: 1em;"><p>The email sink uses the 'smtp' server to publish events via emails. The events can be published in 'text', 'xml' or 'json' formats. The user can define email sink parameters in either the '&lt;SP_HOME&gt;/conf/&lt;PROFILE&gt;/deployment yaml' file or in the stream definition. The email sink first checks the stream definition for parameters, and if they are no configured there, it checks the 'deployment.yaml' file. If the parameters are not configured in either place, default values are considered for optional parameters. If you need to configure server system parameters that are not provided as options in the stream definition, then those parameters need to be defined them in the 'deployment.yaml' file under 'email sink properties'.<br><br>For more information about the SMTP server parameters, see  https://javaee.github.io/javamail/SMTP-Transport.<br><br>Further, some email accounts are required to enable the 'access to less secure apps' option. For gmail accounts, you can enable this option via https://myaccount.google.com/lesssecureapps.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-email/api/1.1.0/#email-source">email</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source">(Source)</a>*<br><div style="padding-left: 1em;"><p>The 'Email' source allows you to receive events via emails. An 'Email' source can be configured using the 'imap' or 'pop3' server to receive events. This allows you to filter the messages that satisfy the criteria specified under the 'search term' option. The email source parameters can be defined in either the '&lt;SP_HOME&gt;/conf/&lt;PROFILE&gt;/deployment yaml' file or the stream definition. If the parameter configurations are not available in either place, the default values are considered (i.e., if default values are available). If you need to configure server system parameters that are not provided as options in the stream definition, they need to be defined in the 'deployment yaml' file under 'email source properties'. For more information about 'imap' and 'pop3' server system parameters, see the following.<br>[JavaMail Reference Implementation - IMAP Store](https://javaee.github.io/javamail/IMAP-Store)<br>[JavaMail Reference Implementation - POP3 Store Store](https://javaee.github.io/javamail/POP3-Store)</p></div>

## How to Contribute
 
  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-email/issues">GitHub Issue Tracker</a>.
  
  * Send your contributions as pull requests to <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-email/tree/master">master branch</a>. 

## Contact us 

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 
 * Siddhi developers can be contacted via the mailing lists:
 
    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)
    
    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)
 
## Support 

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 
