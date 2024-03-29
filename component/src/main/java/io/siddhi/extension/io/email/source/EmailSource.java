/*
 *  Copyright (c) 2017 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package io.siddhi.extension.io.email.source;

import com.sun.mail.util.MailConnectException;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.SystemParameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.email.source.exception.EmailSourceAdaptorRuntimeException;
import io.siddhi.extension.io.email.util.EmailConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.wso2.transport.email.connector.factory.EmailConnectorFactoryImpl;
import org.wso2.transport.email.contract.EmailConnectorFactory;
import org.wso2.transport.email.contract.EmailMessageListener;
import org.wso2.transport.email.contract.EmailServerConnector;
import org.wso2.transport.email.exception.EmailConnectorException;

import java.net.ConnectException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The class implementing Email source.
 */
@Extension(name = "email", namespace = "source",
        description = "The 'Email' source allows you to receive events via emails. An 'Email' source can be " +
                "configured using the 'imap' or 'pop3' server to receive events. This allows you to filter the " +
                "messages that satisfy the criteria specified under the 'search term' option. The email source " +
                "parameters can be defined in either the '<SP_HOME>/conf/<PROFILE>/deployment yaml' file or the " +
                "stream definition. If the parameter configurations are not available in either place, the default " +
                "values are considered (i.e., if default values are available). If you need to configure server " +
                "system parameters that are not provided as options in the stream definition, they need to be " +
                "defined in the 'deployment yaml' file under 'email source properties'. For more information about" +
                " 'imap' and 'pop3' server system parameters, see the following.\n" +
                "[JavaMail Reference Implementation - IMAP Store](https://javaee.github.io/javamail/IMAP-Store)\n" +
                "[JavaMail Reference Implementation - POP3 Store Store](https://javaee.github.io/javamail/POP3-Store)",

        parameters = {
                @Parameter(name = "username",
                        description = "The user name of the email account. e.g., 'wso2mail' is the username of the " +
                                "'wso2mail@gmail.com' mail account.",
                        type = { DataType.STRING}),
                @Parameter(name = "password",
                        description = "The password of the email account",
                        type = { DataType.STRING }),
                @Parameter(name = "store",
                        description = "The store type that used to receive emails. Possible values are 'imap' and " +
                                "'pop3'.",
                        type = {DataType.STRING },
                        optional = true,
                        defaultValue = "imap"),
                @Parameter(name = "host",
                        description = "The host name of the server "
                                + "(e.g., 'imap.gmail.com' is the host name for a gmail account with an IMAP store.). "
                                + "The default value 'imap.gmail.com' is only valid if the email account is a gmail" +
                                " account with IMAP enabled.",
                        type = { DataType.STRING },
                        optional = true,
                        defaultValue = "If store type is 'imap', then the default value is "
                                + "'imap.gmail.com'. If the store type is 'pop3', then the"
                                + "default value is 'pop3.gmail.com'."),
                @Parameter(name = "port",
                        description = "The port that is used to create the connection.",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "'993', the default value is valid only if the store is 'imap' and " +
                                "ssl-enabled."),
                @Parameter(name = "folder",
                        description = "The name of the folder to which the emails should be fetched.",
                        type = { DataType.STRING },
                        optional = true,
                        defaultValue = "INBOX"),
                @Parameter(name = "search.term",
                        description = "The option that includes conditions such as key-value pairs to search for " +
                                "emails. In a string search term, the key and the value should be separated by a " +
                                "semicolon (';'). Each key-value pair must be within inverted commas (' '). The " +
                                "string search term can define multiple comma-separated key-value pairs. This string" +
                                " search term currently supports only the 'subject', 'from', 'to', 'bcc', and 'cc'" +
                                " keys. e.g., if you enter 'subject:DAS, from:carbon, bcc:wso2', the search term " +
                                "creates a search term instance that filters emails that contain 'DAS' in the " +
                                "subject, 'carbon' in the 'from' address, and 'wso2' in one of the 'bcc' addresses." +
                                " The string search term carries out sub string matching that is case-sensitive." +
                                " If '@' in included in the value for any key other than the 'subject' key, it" +
                                " checks for an address that is equal to the value given. e.g., If you search for" +
                                " 'abc@', the string search terms looks for an address that contains 'abc' before the" +
                                " '@' symbol.",
                        type = { DataType.STRING },
                        optional = true,
                        defaultValue = "None"),
                @Parameter(name = "polling.interval",
                        description = "This defines the time interval in seconds at which th email source should poll" +
                                " the account to check for new mail arrivals."
                                + "in seconds.",
                        type = { DataType.LONG },
                        optional = true,
                        defaultValue = "600"),
                @Parameter(name = "action.after.processed",
                        description = "The action to be performed by the email source for the processed mail. " +
                                "Possible values are as follows:\n"
                                + "'FLAGGED': Sets the flag as 'flagged'.\n"
                                + "'SEEN': Sets the flag as 'read'.\n"
                                + "'ANSWERED': Sets the flag as 'answered'.\n"
                                + "'DELETE': Deletes tha mail after the polling cycle.\n"
                                + "'MOVE': Moves the mail to the folder specified in the 'folder.to.move' " +
                                "parameter.\n" +
                                " If the folder specified is 'pop3', then the only option available is 'DELETE'.",
                        type = { DataType.STRING },
                        optional = true,
                        defaultValue = "NONE"),
                @Parameter(name = "folder.to.move",
                        description = "The name of the folder to which the mail must be moved once it is processed." +
                                " If the action after processing is 'MOVE', it is required to specify a value for " +
                                "this parameter.",
                        type = { DataType.STRING }),
                @Parameter(name = "content.type",
                        description = "The content type of the email. It can be either 'text/plain' or 'text/html.'",
                        type = { DataType.STRING },
                        optional = true,
                        defaultValue = "text/plain"),
                @Parameter(name = "ssl.enable",
                        description = "If this is set to 'true', a secure port is used to establish the connection." +
                                " The possible values are 'true' and 'false'.",
                        type = { DataType.BOOL },
                        optional = true,
                        defaultValue = "true") },
        examples = {
                @Example(syntax = "@source(type='email', @map(type='xml'), "
                                + "username='receiver.account', "
                                + "password='account.password',"
                                + ")" +
                                "define stream inputStream (name string, age int, country string);",
                        description = "This example illustrates how to receive events in 'xml' format via the email" +
                                " source. In this example, only the required parameters are defined in the " +
                                "stream definition. The default values are taken for the other parameters. The search" +
                                " term is not defined, and therefore, all the new messages in the inbox folder are " +
                                "polled and taken."
                ),

                @Example(syntax = "@source(type='email', @map(type='xml'), "
                                + "username='receiver.account', "
                                + "password='account.password',"
                                + "store = 'imap',"
                                + "host = 'imap.gmail.com',"
                                + "port = '993',"
                                + "searchTerm = 'subject:Stream Processor, from: from.account@ , cc: cc.account',"
                                + "polling.interval='500',"
                                + "action.after.processed='DELETE',"
                                + "content.type='text/html,"
                                + ")" +
                                "define stream inputStream (name string, age int, country string);",
                        description = "This example illustrates how to receive events in 'xml' format via the email" +
                                " source. The email source polls the mail account every 500 seconds to check whether" +
                                " any new mails have arrived. It processes new mails only if they satisfy the " +
                                "conditions specified for the email search term (the value " +
                                "for 'from' of the email message should be 'from.account@.<host name>', and the " +
                                "message should contain 'cc.account' in the cc receipient list and the word " +
                                "'Stream Processor' in the mail subject). in this example, the action after " +
                                "processing is 'DELETE'. Therefore,after processing the event, corresponding mail is" +
                                " deleted from the mail folder."
                ),
        },
        systemParameter = {
                @SystemParameter(name = "mail.imap.partialfetch",
                        description = "This determines whether the IMAP partial-fetch capability should be used.",
                        defaultValue = "true",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.imap.fetchsize",
                        description = "The partial fetch size in bytes.",
                        defaultValue = "16K",
                        possibleParameters = "value in bytes"),
                @SystemParameter(name = "mail.imap.peek",
                        description = "If this is set to 'true', the IMAP PEEK option should be used when fetching" +
                                " body parts to avoid setting the 'SEEN' flag on messages. The default value is " +
                                "'false'. This can be overridden on a per-message basis by the 'setPeek method' in " +
                                "'IMAPMessage'.",
                        defaultValue = "false",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.imap.connectiontimeout",
                        description = "The socket connection timeout value in milliseconds."
                                + " This timeout is implemented by 'java.net.Socket'.",
                        defaultValue = "infinity timeout",
                        possibleParameters = "Any Integer value"),
                @SystemParameter(name = "mail.imap.timeout",
                        description = "The socket read timeout value in milliseconds. This timeout is implemented by" +
                                " 'java.net.Socket'.",
                        defaultValue = "infinity timeout",
                        possibleParameters = "Any Integer value"),
                @SystemParameter(name = "mail.imap.writetimeout",
                        description = "The socket write timeout value in milliseconds. This timeout is implemented"
                                + " by using a 'java.util.concurrent.ScheduledExecutorService' per connection that "
                                + "schedules a thread to close the socket if the timeout period elapses."
                                + " Therefore, the overhead of using this timeout is one thread per connection.",
                        defaultValue = "infinity timeout",
                        possibleParameters = "Any Integer value"),
                @SystemParameter(name = "mail.imap.statuscachetimeout",
                        description = "The timeout value in milliseconds for the cache of 'STATUS' command response.",
                        defaultValue = "1000ms",
                        possibleParameters = "Time out in miliseconds"),
                @SystemParameter(name = "mail.imap.appendbuffersize",
                        description = "The maximum size of a message to buffer in memory when appending to an IMAP" +
                                " folder.",
                        defaultValue = "None",
                        possibleParameters = "Any Integer value"),
                @SystemParameter(name = "mail.imap.connectionpoolsize",
                        description = "The maximum number of available connections in the connection pool.",
                        defaultValue = "1",
                        possibleParameters = "Any Integer value"),
                @SystemParameter(name = "mail.imap.connectionpooltimeout",
                        description = "The timeout value in milliseconds for connection pool connections. ",
                        defaultValue = "45000ms",
                        possibleParameters = "Any Integer"),
                @SystemParameter(name = "mail.imap.separatestoreconnection",
                        description = "If this parameter is set to 'true', it indicates that a dedicated store " +
                                "connection needs to be used for store commands.",
                        defaultValue = "true",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.imap.auth.login.disable",
                        description = "If this is set to 'true', it is not possible to use the non-standard " +
                                "'AUTHENTICATE LOGIN' command instead of the plain 'LOGIN' command.",
                        defaultValue = "false",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.imap.auth.plain.disable",
                        description = "If this is set to 'true', the 'AUTHENTICATE PLAIN' command cannot be used.",
                        defaultValue = "false",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.imap.auth.ntlm.disable",
                        description = "If true, prevents use of the AUTHENTICATE NTLM command.",
                        defaultValue = "false",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.imap.proxyauth.user",
                        description = "If the server supports the PROXYAUTH extension, this property"
                                + " specifies the name of the user to act as. Authentication to log in to the server"
                                + " is carried out using the administrator's credentials. After authentication,"
                                + " the IMAP provider issues the 'PROXYAUTH' command with the user name specified " +
                                "in this property.",
                        defaultValue = "None",
                        possibleParameters = "Valid string value"),
                @SystemParameter(name = "mail.imap.localaddress",
                        description = "The local address (host name) to bind to when creating the IMAP socket.",
                        defaultValue = "Defaults to the address picked by the Socket class.",
                        possibleParameters = "Valid string value"),
                @SystemParameter(name = "mail.imap.localport",
                        description = "The local port number to bind to when creating the IMAP socket.",
                        defaultValue = "Defaults to the port number picked by the Socket class.",
                        possibleParameters = "Valid String value"),
                @SystemParameter(name = "mail.imap.sasl.enable",
                        description = "If this parameter is set to 'true', the system attempts to use the " +
                                "'javax.security.sasl' package to choose an authentication mechanism for the login.",
                        defaultValue = "false",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.imap.sasl.mechanisms",
                        description = "A list of SASL mechanism names that the system should to try to use. The " +
                                "names can be separated by spaces or commas.",
                        defaultValue = "None",
                        possibleParameters = "Valid string value"),
                @SystemParameter(name = "mail.imap.sasl.authorizationid",
                        description = "The authorization ID to use in the SASL authentication.",
                        defaultValue = "If this parameter is not set, the authentication ID (username) is used.",
                        possibleParameters = "Valid string value"),
                @SystemParameter(name = "mail.imap.sasl.realm",
                        description = "The realm to use with SASL authentication mechanisms that require a realm, "
                                + "such as 'DIGEST-MD5'.",
                        defaultValue = "None",
                        possibleParameters = "Valid string value"),
                @SystemParameter(name = "mail.imap.auth.ntlm.domain",
                        description = "The NTLM authentication domain.",
                        defaultValue = "None",
                        possibleParameters = "Valid string value"),
                @SystemParameter(name = "The NTLM authentication domain.",
                        description = "NTLM protocol-specific flags.",
                        defaultValue = "None",
                        possibleParameters = "Valid integer value"),
                @SystemParameter(name = "mail.imap.socketFactory",
                        description = "If this parameter is set to a class that implements the " +
                                "'javax.net.SocketFactory' interface, this class is used to create IMAP sockets.",
                        defaultValue = "None",
                        possibleParameters = "Valid SocketFactory"),
                @SystemParameter(name = "mail.imap.socketFactory.class",
                        description = "If this parameter is set, it specifies the name of a class that implements the "
                                + "'javax.net.SocketFactory' interface. This class is used to create IMAP sockets.",
                        defaultValue = "None",
                        possibleParameters = "Valid string"),
                @SystemParameter(name = "mail.imap.socketFactory.fallback",
                        description = "If this parameter is set to 'true', failure to create a socket using"
                                + " the specified socket factory class results in the socket being created using"
                                + " the 'java.net.Socket' class. ",
                        defaultValue = "true",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.imap.socketFactory.port",
                        description = "This specifies the port to connect to when using the specified socket factory."
                                + " If this parameter is not set, the default port is used.",
                        defaultValue = "143",
                        possibleParameters = "Valid Integer"),
                @SystemParameter(name = "mail.imap.ssl.checkserveridentity",
                        description = "If this parameter is set to 'true', the system checks the server identity as" +
                                " specified by RFC 2595.",
                        defaultValue = "false",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.imap.ssl.trust",
                        description = "If this parameter is set and a socket factory has not been specified, it " +
                                "enables the use of a 'MailSSLSocketFactory'.\n" +
                                "If this parameter is set to '*', all the hosts are trusted.\n" +
                                "If this parameter specifies list of hosts separated by white spaces, only those " +
                                "hosts are trusted.\n" +
                                "If the parameter is not set to any of the values mentioned above, trust depends on" +
                                " the certificate presented by the server.",
                        defaultValue = "*",
                        possibleParameters = "Valid String"),
                @SystemParameter(name = "mail.imap.ssl.socketFactory",
                        description = "If this parameter is set to a class that extends the " +
                                "'javax.net.ssl.SSLSocketFactory' class this class is used to create IMAP SSL sockets.",
                        defaultValue = "None",
                        possibleParameters = "SSL Socket Factory"),
                @SystemParameter(name = "mail.imap.ssl.socketFactory.class",
                        description = "If this parameter is set, it specifies the name of a class that extends the" +
                                " 'javax.net.ssl.SSLSocketFactory' class. This class is used to create IMAP SSL" +
                                " sockets.",
                        defaultValue = "None",
                        possibleParameters = "Valid String"),
                @SystemParameter(name = "mail.imap.ssl.socketFactory.port",
                        description = "This specifies the port to connect to when using the specified socket factory.",
                        defaultValue = "the default port 993 is used.",
                        possibleParameters = "valid port number"),
                @SystemParameter(name = "mail.imap.ssl.protocols",
                        description = "This specifies the SSL protocols that are enabled for SSL connections."
                                + " The property value is a whitespace-separated list of tokens acceptable"
                                + " to the 'javax.net.ssl.SSLSocket.setEnabledProtocols' method.",
                        defaultValue = "None",
                        possibleParameters = "Valid string"),
                @SystemParameter(name = "mail.imap.starttls.enable",
                        description = "If this parameter is set to 'true', it is possible to use the 'STARTTLS' command"
                                + " (if supported by the server) to switch the connection to a"
                                + " TLS-protected connection before issuing any login commands.",
                        defaultValue = "false",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.imap.socks.host",
                        description = "This specifies the host name of a 'SOCKS5' proxy server that is"
                                + " used to connect to the mail server.",
                        defaultValue = "None",
                        possibleParameters = "Valid String"),
                @SystemParameter(name = "mail.imap.socks.port",
                        description = "This specifies the port number for the 'SOCKS5' proxy server."
                                + " This is needed if the proxy server is not using the standard"
                                + " port number 1080.",
                        defaultValue = "1080",
                        possibleParameters = "Valid String"),
                @SystemParameter(name = "mail.imap.minidletime",
                        description = "This property sets the delay in milliseconds.",
                        defaultValue = "10 milliseconds",
                        possibleParameters = "time in seconds (Integer)"),
                @SystemParameter(name = "mail.imap.enableimapevents",
                        description = "If this property is set to 'true', it enables special IMAP-specific events " +
                                "to be delivered to the 'ConnectionListener' of the store. The unsolicited responses" +
                                " received during the idle method of the store are sent as connection events with " +
                                "'IMAPStore.RESPONSE' as the type."
                                + " The event's message is the raw IMAP response string.",
                        defaultValue = "false",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.imap.folder.class",
                        description = "The class name of a subclass of 'com.sun.mail.imap.IMAPFolder'."
                                + " The subclass can be used to provide support for additional IMAP commands."
                                + " The subclass must have public constructors of the form 'public"
                                + " MyIMAPFolder'(String fullName, char separator,"
                                + " IMAPStore store, Boolean isNamespace) "
                                + "and public 'MyIMAPFolder'(ListInfo li, IMAPStore store)",
                        defaultValue = "None",
                        possibleParameters = "Valid String"),
                @SystemParameter(name = "mail.pop3.connectiontimeout",
                        description = "The socket connection timeout value in milliseconds.",
                        defaultValue = "Infinite timeout",
                        possibleParameters = "Integer value"),
                @SystemParameter(name = "mail.pop3.timeout",
                        description = "The socket I/O timeout value in milliseconds. ",
                        defaultValue = "Infinite timeout",
                        possibleParameters = "Integer value"),
                @SystemParameter(name = "mail.pop3.message.class",
                        description = "The class name of a subclass of 'com.sun.mail.pop3.POP3Message'.",
                        defaultValue = "None",
                        possibleParameters = "Valid String"),
                @SystemParameter(name = "mail.pop3.localaddress",
                        description = "The local address (host name) to bind to when creating the POP3 socket.",
                        defaultValue = "Defaults to the address picked by the Socket class.",
                        possibleParameters = "Valid String"),
                @SystemParameter(name = "mail.pop3.localport",
                        description = "The local port number to bind to when creating the POP3 socket.",
                        defaultValue =  "Defaults to the port number picked by the Socket class.",
                        possibleParameters = "Valid port number"),
                @SystemParameter(name = "mail.pop3.apop.enable",
                        description = "If this parameter is set to 'true', use 'APOP' instead of 'USER/PASS' to log" +
                                " in to the 'POP3' server"
                                + " (if the 'POP3' server supports 'APOP'). APOP sends a digest of the password"
                                + " instead of clearing the text password.",
                        defaultValue = "false",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.pop3.socketFactory",
                        description = "If this parameter is set to a class that implements the " +
                                "'javax.net.SocketFactory' interface, this class is used to create 'POP3' sockets.",
                        defaultValue = "None",
                        possibleParameters = "Socket Factory"),
                @SystemParameter(name = "mail.pop3.socketFactory.class",
                        description = "If this parameter is set, it specifies the name of a class that implements " +
                                "the 'javax.net.SocketFactory' interface. "
                                + "This class is used to create 'POP3' sockets.",
                        defaultValue = "None",
                        possibleParameters = "Valid String"),
                @SystemParameter(name = "mail.pop3.socketFactory.fallback",
                        description = "If this parameter is set to 'true', failure to create a socket using the " +
                                "specified socket factory class results in the socket being created using the " +
                                "'java.net.Socket' class.",
                        defaultValue = "false",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.pop3.socketFactory.port",
                        description = "This specifies the port to connect to when using the specified socket factory.",
                        defaultValue = "Default port",
                        possibleParameters = "Valid port number"),
                @SystemParameter(name = "mail.pop3.ssl.checkserveridentity",
                        description = "If this parameter is set to 'true', check the server identity as specified by" +
                                " RFC 2595. ",
                        defaultValue = "false",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.pop3.ssl.trust",
                        description = "If this parameter is set and a socket factory has not been specified, it " +
                                "is possible to use a 'MailSSLSocketFactory'. \n"
                                + "If this parameter is set to '*', all the hosts are trusted.\n"
                                + "If the parameter is set to a whitespace-separated list of hosts, only those hosts " +
                                "are trusted.\n"
                                + "If the parameter is not set to any of the values mentioned above, trust depends" +
                                " on the certificate presented by the server.",
                        defaultValue = "*",
                        possibleParameters = "Valid String"),
                @SystemParameter(name = "mail.pop3.ssl.socketFactory",
                        description = "If this parameter is set to a class that extends the " +
                                "'javax.net.ssl.SSLSocketFactory' class, this class is used to create 'POP3'" +
                                " SSL sockets.",
                        defaultValue = "None",
                        possibleParameters = "SSL Socket Factory"),
                @SystemParameter(name = "mail.pop3.ssl.checkserveridentity",
                        description = "If this parameter is set to 'true', the system checks the server identity as" +
                                " specified by 'RFC 2595'. ",
                        defaultValue = "false",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.pop3.ssl.trust",
                        description = " If this parameter is set and a socket factory has not been specified, it is" +
                                " possible to use a 'MailSSLSocketFactory'.\n"
                                + "If this parameter is set to '*', all the hosts are trusted.\n"
                                + "If the parameter is set to a whitespace-separated list of hosts, only those hosts" +
                                " are trusted.",
                        defaultValue = "Trust depends on the certificate presented by the server.",
                        possibleParameters = "Valid String"),
                @SystemParameter(name = "mail.pop3.ssl.socketFactory",
                        description = "If this parameter is set to a class that extends the " +
                                "'javax.net.ssl.SSLSocketFactory' class, this class is used to create 'POP3 SSL'" +
                                " sockets.",
                        defaultValue = "None",
                        possibleParameters = "SSL Socket Factory"),
                @SystemParameter(name = "mail.pop3.ssl.socketFactory.class",
                        description = "If this parameter is set, it specifies the name of a class that extends"
                                + " the 'javax.net.ssl.SSLSocketFactory' class."
                                + " This class is used to create 'POP3 SSL' sockets. ",
                        defaultValue = "None",
                        possibleParameters = "Valid String"),
                @SystemParameter(name = "mail.pop3.ssl.socketFactory.p",
                        description = "This parameter pecifies the port to connect to when using the specified " +
                                "socket factory.",
                        defaultValue = "995",
                        possibleParameters = "Valid Integer"),
                @SystemParameter(name = "mail.pop3.ssl.protocols",
                        description = "This parameter specifies the SSL protocols that are enabled for SSL connections."
                                + " The property value is a whitespace-separated list of tokens acceptable"
                                + " to the 'javax.net.ssl.SSLSocket.setEnabledProtocols' method.",
                        defaultValue = "None",
                        possibleParameters = "Valid String"),
                @SystemParameter(name = "mail.pop3.starttls.enable",
                        description = "If this parameter is set to 'true', it is possible to use the 'STLS' command" +
                                " (if supported by the server) to switch the connection to a TLS-protected connection" +
                                " before issuing any login commands.",
                        defaultValue = "false",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.pop3.starttls.required",
                        description = "If this parameter is set to 'true', it is required to use the 'STLS' command." +
                                " The connect method fails if the server does not support the 'STLS' command or if " +
                                "the command fails.",
                        defaultValue = "false",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.pop3.socks.host",
                        description = "This parameter specifies the host name of a 'SOCKS5' proxy server that can " +
                                "be used to connect to the mail server.",
                        defaultValue = "None",
                        possibleParameters = "Valid String"),
                @SystemParameter(name = "mail.pop3.socks.port",
                        description = "This parameter specifies the port number for the 'SOCKS5' proxy server.",
                        defaultValue = "None",
                        possibleParameters = "Valid String"),
                @SystemParameter(name = "mail.pop3.disabletop",
                        description = "If this parameter is set to 'true', the 'POP3 TOP' command is not used to"
                                + " fetch message headers. ",
                        defaultValue = "false",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.pop3.forgettopheaders",
                        description = "If this parameter is set to 'true', the headers that might have been " +
                                "retrieved using the 'POP3 TOP' command is forgotten and replaced by the headers" +
                                " retrieved when the 'POP3 RETR' command is executed.",
                        defaultValue = "false",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.pop3.filecache.enable",
                        description = "If this parameter is set to 'true', the 'POP3' provider caches message data in" +
                                " a temporary file instead of caching them in memory. Messages are only added to the" +
                                " cache when accessing the message content. Message headers are always cached in " +
                                "memory (on demand). The file cache is removed when the folder is closed or the JVM" +
                                " terminates.",
                        defaultValue = "false",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.pop3.filecache.dir",
                        description = "If the file cache is enabled, this property is used to override the default" +
                                " directory used by the JDK for temporary files.",
                        defaultValue = "None",
                        possibleParameters = "Valid String"),
                @SystemParameter(name = "mail.pop3.cachewriteto",
                        description = "This parameter controls the behavior of the 'writeTo' method on a 'POP3' " +
                                "message object. If the parameter is set to 'true', the message content has not been" +
                                " cached yet, and the 'ignoreList' is null, the message is cached before being " +
                                "written. If not, the message is streamed directly to the output stream without being" +
                                " cached.",
                        defaultValue = "false",
                        possibleParameters = "true or false"),
                @SystemParameter(name = "mail.pop3.keepmessagecontent",
                        description = "If this property is set to 'true', a hard reference to the cached content"
                                + " is retained, preventing the memory from being reused until the folder"
                                + " is closed, or until the cached content is explicitly invalidated (using the " +
                                "'invalidate' method). ",
                        defaultValue = "false",
                        possibleParameters = "true or false"),
        })

public class EmailSource extends Source {

    private static final Logger log = LogManager.getLogger(EmailSource.class);
    private SourceEventListener sourceEventListener;
    private ConfigReader configReader;
    private OptionHolder optionHolder;
    private EmailServerConnector emailServerConnector;
    private EmailMessageListener emailMessageListener;
    private Map<String, String> properties = new HashMap<>();
    private String store;
    private String contentType;
    private boolean isImap = false;

    /**
     * The initialization method for {@link Source}, which will be called before other methods and validate
     * the all configuration and getting the intial values.
     * @param sourceEventListener After receiving events, the source should trigger onEvent() of this listener.
     *                            Listener will then pass on the events to the appropriate mappers for processing .
     * @param optionHolder        Option holder containing static configuration related to the {@link Source}
     * @param configReader        to read the {@link Source} related system configuration.
     * @param siddhiAppContext    the context of the {@link io.siddhi.query.api.SiddhiApp} used to get siddhi
     *                            related utilty functions.
     */
    @Override public StateFactory init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                                       String[] requiredProperties, ConfigReader configReader,
                                       SiddhiAppContext siddhiAppContext) {
        this.sourceEventListener = sourceEventListener;
        this.configReader = configReader;
        this.optionHolder = optionHolder;
        validateAndGetEmailConfigurations();
        //Server system properties starts with 'mail.smtp'.
        configReader.getAllConfigs().forEach((k, v) -> {
            if (k.startsWith("mail." + store)) {
                properties.put(k, v);
            }
        });
        properties.put(EmailConstants.TRANSPORT_MAIL_AUTO_ACKNOWLEDGE, EmailConstants.DEFAULT_AUTO_ACKNOWLEDGE);
        EmailConnectorFactory emailConnectorFactory = new EmailConnectorFactoryImpl();
            try {
                    emailServerConnector = emailConnectorFactory.createEmailServerConnector(
                            "emailSource", properties);
            } catch (EmailConnectorException e) {
                    throw new EmailSourceAdaptorRuntimeException("Error is encountered while creating the email "
                            + "server connector.", e);
            }

            emailMessageListener = new EmailSourceMessageListener(sourceEventListener,
                    requiredProperties, contentType);
            return null;
    }

    /**
     * Intialy Called to connect to the end point for start  retriving the messages asynchronisly .
     *
     * @param connectionCallback Callback to pass the ConnectionUnavailableException in case of connection failure after
     *                           initial successful connection(can be used when events are receving asynchronasily)
     * @throws ConnectionUnavailableException if it cannot connect to the source backend immediately.
     */
    @Override public void connect(ConnectionCallback connectionCallback, State state)
            throws ConnectionUnavailableException {
        try {
            emailServerConnector.init();
            emailServerConnector.start(emailMessageListener);
        } catch (EmailConnectorException e) {
            //calling super class logs the exception and retry
            if (e.getCause() instanceof MailConnectException) {
                if (e.getCause().getCause() instanceof ConnectException) {
                    throw new ConnectionUnavailableException(
                            "Connection is unavailable. Therefore retry again" + " to connect to the store."
                                    + e.getMessage(), e.getCause());
                } else {
                    throw new EmailSourceAdaptorRuntimeException(
                            "Error is encountered while connecting" + " the Email Source for stream: "
                                    + sourceEventListener.getStreamDefinition()
                                    + "." + e.getMessage(), e.getCause());
                }
            } else {
                throw new EmailSourceAdaptorRuntimeException("Couldn't connect to email server connector. Therefore, "
                        + "exist from the Siddhi App execution." + e.getMessage(), e);
            }
        }
    }

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    /**
     * This method can be called when it is needed to disconnect from the end point.
     */
    @Override public void disconnect() {
        try {
            if (emailServerConnector != null) {
                emailServerConnector.stop();
            }

        } catch (EmailConnectorException e) {
            throw new EmailSourceAdaptorRuntimeException(
                    "Error is encountered while disconnecting " + "the Email Source for stream: "
                            + sourceEventListener.getStreamDefinition() + "." + e.getMessage(), e);
        }
    }

    /**
     * Called at the end to clean all the resources consumed by the {@link Source}
     */
    @Override public void destroy() {
        if (emailServerConnector != null) {
            try {
                emailServerConnector.stop();
            } catch (EmailConnectorException e) {
                log.error("Error is encountered while destroying Email Source for stream: "
                        + sourceEventListener.getStreamDefinition() + "." + e.getMessage(), e);
            }
        }
    }

    /**
     * Called to pause event consumption
     */
    @Override public void pause() {
        if (emailServerConnector != null) {
            try {
                emailServerConnector.stop();
            } catch (EmailConnectorException e) {
                throw new EmailSourceAdaptorRuntimeException(
                        "Error is encountered while pausing" + " the Email Source." + e.getMessage(), e);
            }
        }
    }

    /**
     * Called to resume event consumption
     */
    @Override public void resume() {
        if (emailServerConnector != null) {
            try {
                emailServerConnector.start(emailMessageListener);
            } catch (EmailConnectorException e) {
                throw new EmailSourceAdaptorRuntimeException(
                        "Error is encountered while resuming" + " the Email Source." + e.getMessage(), e);
            }
        }
    }

    /**
     * Returns the list of classes which this source can output.
     *
     * @return Array of classes that will be output by the source.
     * Null or empty array if it can produce any type of class.
     */
    @Override public Class[] getOutputEventClasses() {
            return new Class[] { String.class };
    }

    /**
     * Get the email parameters and validate them. If they are defined in correct way then they are put into the
     * email property map else throw SiddhiAppCreation exception.
     */
    private void validateAndGetEmailConfigurations() {

        String username = optionHolder.validateAndGetStaticValue(EmailConstants.EMAIL_RECEIVER_USERNAME,
                configReader.readConfig(EmailConstants.EMAIL_RECEIVER_USERNAME, EmailConstants.EMPTY_STRING));
        if (!username.isEmpty()) {
            properties.put(EmailConstants.TRANSPORT_MAIL_RECEIVER_USERNAME, username);
        } else {
            throw new SiddhiAppCreationException(EmailConstants.EMAIL_RECEIVER_USERNAME + " is a mandatory parameter. "
                    + "It should be defined in either stream definition or deployment 'yaml' file.");
        }

        String password = optionHolder.validateAndGetStaticValue(EmailConstants.EMAIL_RECEIVER_PASSWORD,
                configReader.readConfig(EmailConstants.EMAIL_RECEIVER_PASSWORD, EmailConstants.EMPTY_STRING));
        if (!password.isEmpty()) {
            properties.put(EmailConstants.TRANSPORT_MAIL_RECEIVER_PASSWORD, password);
        } else {
            throw new SiddhiAppCreationException(EmailConstants.EMAIL_RECEIVER_PASSWORD + " is a mandatory parameter. "
                    + "It should be defined in either stream definition or deployment 'yaml' file.");
        }

        this.store = optionHolder.validateAndGetStaticValue(EmailConstants.STORE,
                configReader.readConfig(EmailConstants.STORE, EmailConstants.EMAIL_RECEIVER_DEFAULT_STORE));
        if (!store.contains(EmailConstants.IMAP_STORE) && !store.contains(EmailConstants.POP3_STORE)) {
            throw new SiddhiAppCreationException(
                    EmailConstants.STORE + " could be either " + EmailConstants.IMAP_STORE + " or "
                            + EmailConstants.POP3_STORE + ". But found: " + store + ".");
        }
        properties.put(EmailConstants.TRANSPORT_MAIL_STORE, store);
        if (store.contains(EmailConstants.IMAP_STORE)) {
            isImap = true;
        }

        String host = optionHolder.validateAndGetStaticValue(EmailConstants.EMAIL_RECEIVER_HOST,
                configReader.readConfig(EmailConstants.EMAIL_RECEIVER_HOST, store + ".gmail.com"));
        properties.put(EmailConstants.TRANSPORT_MAIL_RECEIVER_HOST_NAME, host);

        String sslEnable = optionHolder.validateAndGetStaticValue(EmailConstants.EMAIL_RECEIVER_SSL_ENABLE, configReader
                .readConfig(EmailConstants.EMAIL_RECEIVER_SSL_ENABLE,
                        EmailConstants.EMAIL_RECEIVER_DEFAULT_SSL_ENABLE));
        if (!(sslEnable.equalsIgnoreCase("true") || sslEnable.equalsIgnoreCase("false"))) {
            throw new SiddhiAppCreationException(
                    EmailConstants.STORE + "could be either 'true' " + "or 'false'. But found: " + sslEnable);
        }
        properties.put("mail." + store + ".ssl.enable", sslEnable);

        String port = optionHolder.validateAndGetStaticValue(EmailConstants.EMAIL_RECEIVER_PORT, configReader
                .readConfig(EmailConstants.EMAIL_RECEIVER_PORT, EmailConstants.EMPTY_STRING));
        if (port.isEmpty()) {
                if (sslEnable.equalsIgnoreCase("true") && store.equalsIgnoreCase("imap")) {
                        port = EmailConstants.EMAIL_RECEIVER_DEFAULT_PORT;
                } else {
                         throw new SiddhiAppCreationException(
                                    "Default value for the port can be only used if 'ssl.enable'"
                                            + " is 'true' and store type is 'imap' only.");
                }
        }
        properties.put("mail." + store + ".port", port);

        String pollingInterval = optionHolder.validateAndGetStaticValue(EmailConstants.POLLING_INTERVAL,
                configReader.readConfig(EmailConstants.POLLING_INTERVAL, EmailConstants.DEFAULT_POLLING_INTERVAL));
        Long timeInMilliSeconds = Duration.of(Long.parseLong(pollingInterval), ChronoUnit.SECONDS).toMillis();
        properties.put(EmailConstants.TRANSPORT_MAIL_POLLING_INTERVAL, Long.toString(timeInMilliSeconds));

        //get a list of valid search term keys.
        List<String> validSearchTermKeys = Stream.of(EmailConstants.SearchTermKeys.values()).
                map(EmailConstants.SearchTermKeys::name).collect(Collectors.toList());
        List<String> givenSearchtermkeys = new ArrayList<>();
        String searchTerm = optionHolder.validateAndGetStaticValue(EmailConstants.EMAIL_SEARCH_TERM,
                configReader.readConfig(EmailConstants.EMAIL_SEARCH_TERM, EmailConstants.EMPTY_STRING));

        String pattern = "^(([ ]*[a-zA-Z]*[ ]*:[^:,]*,[ ]*)*[ ]*[a-zA-Z]*[ ]*:[^:,]*$)";

        if (!searchTerm.isEmpty()) {
            if (!(searchTerm.matches(pattern))) {
                throw new SiddhiAppCreationException("search term '" + searchTerm + "'"
                        + " is not in correct format. It should be in 'key1:value1,key2:value2, ..."
                        + ", keyX:valueX format.");
            } else {
                String condition[] = searchTerm.split(",");
                for (int i = 0; i < condition.length; i++) {
                    String[] nameValuePair = condition[i].split(":");
                    if (nameValuePair.length == 2) {
                        givenSearchtermkeys.add(nameValuePair[0].trim().toUpperCase(Locale.ENGLISH));
                    } else {
                        throw new SiddhiAppCreationException("The given key value pair '" + nameValuePair[i]
                                + "' in string search term is not in the correct format.");
                    }
                }
            }
            //check given search term keys are valid.
            if (!validSearchTermKeys.containsAll(givenSearchtermkeys)) {
                throw new SiddhiAppCreationException("Valid search term to search emails are" +
                        " 'subject, bcc, cc, to and from' only. But found: "
                        + givenSearchtermkeys.toString());
            }
            properties.put(EmailConstants.TRANSPORT_MAIL_SEARCH_TERM, searchTerm);
        }

        String folder = optionHolder.validateAndGetStaticValue(EmailConstants.FOLDER,
                configReader.readConfig(EmailConstants.FOLDER, EmailConstants.DEFAULT_FOLDER));
        properties.put(EmailConstants.TRANSPORT_MAIL_FOLDER_NAME, folder);

        //get a list of valid action after processed.
        List<String> validActions = Stream.of(EmailConstants.ActionAfterProcessed.values())
                .map(EmailConstants.ActionAfterProcessed::name).collect(Collectors.toList());

        String action;
        if (isImap) {
            action = optionHolder.validateAndGetStaticValue(EmailConstants.ACTION_AFTER_PROCESSED,
                    configReader.readConfig(EmailConstants.ACTION_AFTER_PROCESSED, "SEEN"));
            if (!validActions.contains(action.toUpperCase(Locale.ENGLISH))) {
                throw new SiddhiAppCreationException(EmailConstants.ACTION_AFTER_PROCESSED
                        + " could be 'MOVE, DELETE , SEEN, FLAGGED, ANSWERED,'. But found: " + action);
            }
        } else {
            action = optionHolder.validateAndGetStaticValue(EmailConstants.ACTION_AFTER_PROCESSED,
                    configReader.readConfig(EmailConstants.ACTION_AFTER_PROCESSED, "DELETE"));
            if (!action.equalsIgnoreCase("DELETE")) {
                throw new SiddhiAppCreationException(EmailConstants.ACTION_AFTER_PROCESSED + ""
                        + " could only be 'DELETE' for the pop3 folder. But found: " + action);
            }
        }
        properties.put(EmailConstants.TRANSPORT_MAIL_ACTION_AFTER_PROCESSED, action);

        String moveToFolder = optionHolder.validateAndGetStaticValue(EmailConstants.MOVE_TO_FOLDER,
                configReader.readConfig(EmailConstants.MOVE_TO_FOLDER, EmailConstants.EMPTY_STRING));
        if (action.equalsIgnoreCase("MOVE")) {
            if (moveToFolder.isEmpty()) {
                throw new SiddhiAppCreationException(
                        "Since action after processed mail is 'MOVE', it " + "is mandatory to define "
                                + EmailConstants.ACTION_AFTER_PROCESSED + "parameter "
                                + "in either stream definition or deployment 'yaml' file.");
            } else if (moveToFolder.equals(folder)) {
                log.warn("Given folder '" + moveToFolder + "' to move mails after processing"
                        + " has the same name of email going to fetch. Therefore, emails are"
                        + " remaining in the same folder.");
            }
        } else {
            if (!moveToFolder.isEmpty()) {
                log.warn("Since action after processed mail is '" + action + "'."
                        + " The given folder name to move mails" + moveToFolder
                        + "is neglected while SiddhiAppCreation.");
            }
        }
        properties.put(EmailConstants.TRANSPORT_MAIL_MOVE_TO_FOLDER, moveToFolder);

        this.contentType = optionHolder.validateAndGetStaticValue(EmailConstants.EMAIL_RECEIVER_CONTENT_TYPE,
                configReader.readConfig(EmailConstants.EMAIL_RECEIVER_CONTENT_TYPE,
                        EmailConstants.EMAIL_RECEIVER_DEFAULT_CONTENT_TYPE));
        if (!(contentType.equalsIgnoreCase(EmailConstants.TEXT_HTML) ||
                contentType.equalsIgnoreCase(EmailConstants.TEXT_PLAIN))) {
            throw new SiddhiAppCreationException("supported content types are '" + EmailConstants.TEXT_HTML
            + "' and '" + EmailConstants.TEXT_PLAIN + "' but found: " + contentType + ".");
        }
        properties.put(EmailConstants.TRANSPORT_MAIL_RECEIVER_CONTENT_TYPE, contentType);

        //Default we trust all the hosts (imap and pop3 servers). If user need to trust set of hosts then,
        // it is required to set 'ssl.trust' system property in deployment yaml under email source configuration.
        String trust = configReader.readConfig("mail." + store + "." + EmailConstants.EMAIL_RECEIVER_TRUST,
                EmailConstants.EMAIL_RECEIVER_DEFAULT_TRUST);
        properties.put("mail." + store + "." + EmailConstants.EMAIL_RECEIVER_TRUST, trust);
    }
}
