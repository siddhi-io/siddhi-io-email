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

package io.siddhi.extension.io.email.sink;

import com.sun.mail.smtp.SMTPSendFailedException;
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
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.Option;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.email.sink.transport.EmailClientConnectionPoolManager;
import io.siddhi.extension.io.email.util.EmailConstants;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.wso2.transport.email.connector.factory.EmailConnectorFactoryImpl;
import org.wso2.transport.email.contract.EmailClientConnector;
import org.wso2.transport.email.contract.EmailConnectorFactory;
import org.wso2.transport.email.contract.message.EmailBaseMessage;
import org.wso2.transport.email.contract.message.EmailMultipartMessage;
import org.wso2.transport.email.contract.message.EmailTextMessage;
import org.wso2.transport.email.exception.EmailConnectorException;

import java.net.ConnectException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This the class implementing email sink.
 */
@Extension(
        name = "email",
        namespace = "sink",
        description = "The email sink uses the 'smtp' server to publish events via emails. The events can be " +
                "published in 'text', 'xml' or 'json' formats. The user can define email sink parameters in either " +
                "the '<SP_HOME>/conf/<PROFILE>/deployment yaml' file or in the stream definition. The email sink " +
                "first checks the stream definition for parameters, and if they are no configured there, it checks " +
                "the 'deployment.yaml' file. If the parameters are not configured in either place, default values " +
                "are considered for optional parameters. If you need to configure server system parameters that are " +
                "not provided as options in the stream definition, then those parameters need to be defined them in " +
                "the 'deployment.yaml' file under 'email sink properties'.\n" +
                "\n" +
                "For more information about the SMTP server parameters, see  " +
                "https://javaee.github.io/javamail/SMTP-Transport.\n" +
                "\n" +
                "Further, some email accounts are required to enable the 'access to less secure apps' option. For " +
                "gmail accounts, you can enable this option via https://myaccount.google.com/lesssecureapps.",
        parameters = {
                @Parameter(name = "username",
                           description = "The username of the email account that is used to send emails.\n" +
                                   "e.g., 'abc' is the username of the 'abc@gmail.com' account.",
                           type = {DataType.STRING}),
                @Parameter(name = "address",
                           description = "The address of the email account that is used to send emails.",
                           type = {DataType.STRING}),
                @Parameter(name = "password",
                           description = "The password of the email account.",
                           type = {DataType.STRING}),
                @Parameter(name = "host",
                           description = "The host name of the SMTP server. e.g., 'smtp.gmail.com' is a host name for" +
                                   " a gmail account. The default value 'smtp.gmail.com' is only valid if the email " +
                                   "account is a gmail account.",
                           type = {DataType.STRING},
                           optional = true,
                           defaultValue = "smtp.gmail.com"),
                @Parameter(name = "port",
                           description = "The port that is used to create the connection.",
                           type = {DataType.INT},
                           optional = true,
                           defaultValue = "'465' the default value is only valid is SSL is enabled."),
                @Parameter(name = "ssl.enable",
                           description = "This parameter specifies whether the connection should be established via " +
                                   "a secure connection or not. The value can be either 'true' or 'false'. If it is" +
                                   " 'true', then the connection is establish via the  493 port which is a secure" +
                                   " connection.",
                           type = {DataType.BOOL},
                           optional = true,
                           defaultValue = "true"),
                @Parameter(name = "auth",
                           description = "This parameter specifies whether to use the 'AUTH' command when " +
                                   "authenticating or not. If the parameter is set to 'true', an attempt is made to " +
                                   "authenticate the user using the 'AUTH' command.",
                           type = {DataType.BOOL},
                           optional = true,
                           defaultValue = "true"),
                @Parameter(name = "content.type",
                           description = "The content type can be either 'text/plain' or 'text/html'.",
                           type = {DataType.STRING},
                           optional = true,
                           defaultValue = "text/plain"),
                @Parameter(name = "subject",
                           description = "The subject of the mail to be send.",
                           type = {DataType.STRING},
                           dynamic = true),
                @Parameter(name = "to",
                           description = "The address of the 'to' recipient. If there are more than one 'to' " +
                                   "recipients, then all the required addresses can be given as a comma-separated " +
                                   "list.",
                           type = {DataType.STRING},
                           dynamic = true),
                @Parameter(name = "cc",
                           description = "The address of the 'cc' recipient. If there are more than one 'cc' " +
                                   "recipients, then all the required addresses can be given as a comma-separated " +
                                   "list.",
                           type = DataType.STRING,
                           optional = true,
                           defaultValue = "None"),
                @Parameter(name = "bcc",
                           description = "The address of the 'bcc' recipient. If there are more than one 'bcc' " +
                                   "recipients, then all the required addresses can be given as a comma-separated " +
                                   "list.",
                           type = DataType.STRING,
                           optional = true,
                           defaultValue = "None"),
                @Parameter(name = "attachments",
                        description = "File paths of the files that need to be attached to the email.\n" +
                                "These paths should be absolute paths.\n" +
                                "They can be either directories or files\n. " +
                                "If the path is to a directory, all the files located at the first level (i.e., not" +
                                " within another sub directory) are attached.",
                        type = DataType.STRING,
                        optional = true,
                        dynamic = true,
                        defaultValue = "None"),
                @Parameter(
                        name = "connection.pool.size",
                        description = "Number of concurrent Email client connections.",
                        type = DataType.INT,
                        optional = true,
                        defaultValue = "1")
        },
        examples = {
                @Example(syntax = "@sink(type='email', @map(type ='json'), "
                                + "username='sender.account', "
                                + "address='sender.account@gmail.com',"
                                + "password='account.password',"
                                + "subject='Alerts from Wso2 Stream Processor',"
                                + "to='{{email}}',"
                                + ")"
                                + "define stream FooStream (email string, loginId int, name string);",
                        description = "This example illustrates how to publish events via an email sink based on " +
                                "the values provided for the mandatory parameters. As shown in the example, it " +
                                "publishes events from the 'FooStream' in 'json' format as emails to the specified" +
                                " 'to' recipients via the email sink. The email is sent from the " +
                                "'sender.account@gmail.com' email address via a secure connection."),

                @Example(syntax =  "@sink(type='email', @map(type ='json'), "
                                + "subject='Alerts from Wso2 Stream Processor',"
                                + "to='{{email}}',"
                                + ")"
                                + "define stream FooStream (email string, loginId int, name string);",

                        description = "This example illustrates how to configure the query parameters and the system" +
                                " parameters in the 'deployment.yaml' file.\n " +
                                "Corresponding parameters need to be configured under 'email', and namespace:'sink'" +
                                " as follows:\n"
                                + "  siddhi: "
                                + "    extensions:\n"
                                + "      - extension:\n"
                                + "          name:'email'\n"
                                + "          namespace:'sink'\n"
                                + "          properties:\n"
                                + "            username: <sender's email username>\n"
                                + "            address: <sender's email address>\n"
                                + "            password: <sender's email password>\n"
                                + "\n" +
                                "As shown in the example, events from the FooStream are published in 'json' format " +
                                "via the email sink as emails to the given 'to' recipients."
                                + " The email is sent from the 'sender.account@gmail.com' address via a secure " +
                                "connection."
                ),

                @Example(syntax =  "@sink(type='email', @map(type ='json'), "
                                + "username='sender.account', "
                                + "address='sender.account@gmail.com',"
                                + "password='account.password',"
                                + "host='smtp.gmail.com',"
                                + "port='465',"
                                + "ssl.enable='true',"
                                + "auth='true',"
                                + "content.type='text/html',"
                                + "subject='Alerts from Wso2 Stream Processor-{{name}}',"
                                + "to='to1.account@gmail.com, to2.account@gmail.com',"
                                + "cc='cc1.account@gmail.com, cc2.account@gmail.com',"
                                + "bcc='bcc1.account@gmail.com"
                                + ")"
                                + "define stream FooStream (name string, age int, country string);",
                        description = "This example illustrates how to publish events via the email sink."
                                + " Events from the 'FooStream' stream  are published in 'xml' format via the email" +
                                " sink as a text/html message and sent to the specified 'to', 'cc', and 'bcc' " +
                                "recipients via a secure connection. The 'name' namespace in the 'subject' attribute" +
                                " is the value of the 'name' parameter in the corresponding output event."
                ),

                @Example(syntax =  "@sink(type='email', @map(type ='json'), "
                                + "username='sender.account', "
                                + "address='sender.account@gmail.com',"
                                + "password='account.password',"
                                + "host='smtp.gmail.com',"
                                + "port='465',"
                                + "ssl.enable='true',"
                                + "auth='true',"
                                + "content.type='text/html',"
                                + "subject='Alerts from Wso2 Stream Processor-{{name}}',"
                                + "to='to1.account@gmail.com, to2.account@gmail.com',"
                                + "cc='cc1.account@gmail.com, cc2.account@gmail.com',"
                                + "bcc='bcc1.account@gmail.com"
                                + "attachments= '{{attachments}}'"
                                + ")"
                                + "define stream FooStream (name string, age int, country string, attachments string)"
                                + ";",
                        description = "This example illustrates how to publish events via the email sink. Here, the" +
                                " email also contains attachments.\n"
                                + " Events from the FooStream are published in 'xml' format via the email sink as a" +
                                " 'text/html' message to the specified 'to','cc', and 'bcc' recipients via a secure" +
                                " connection. The 'name' namespace in the 'subject' attribute is the value for the" +
                                " 'name' parameter in the corresponding output event.\n"
                                + "The attachments included in the email message are the local files available in " +
                                "the path specified as the value for the 'attachments' attribute."
                ),
        },
        systemParameter = {
                @SystemParameter(name = "mail.smtp.ssl.trust",
                                 description = "If this parameter is se, and a socket factory has not been " +
                                         "specified, it enables the use of a MailSSLSocketFactory. If this parameter" +
                                         " is set to \"*\", all the hosts are trusted. If it is set to a " +
                                         "whitespace-separated list of hosts, only those specified hosts are trusted." +
                                         " If not, the hosts trusted depends on the certificate presented by the " +
                                         "server.",
                                 defaultValue = "*",
                                 possibleParameters = "String"),
                @SystemParameter(name = "mail.smtp.connectiontimeout",
                                 description = "The socket connection timeout value in milliseconds. ",
                                 defaultValue = "infinite timeout",
                                 possibleParameters = "Any Integer"),
                @SystemParameter(name = "mail.smtp.timeout",
                                 description = "The socket I/O timeout value in milliseconds. ",
                                 defaultValue = "infinite timeout",
                                 possibleParameters = "Any Integer"),
                @SystemParameter(name = "mail.smtp.from",
                                 description = "The email address to use for the SMTP MAIL command. "
                                         + "This sets the envelope return address.",
                                 defaultValue = "Defaults to msg.getFrom() "
                                         + "or InternetAddress.getLocalAddress().",
                                 possibleParameters = "Any valid email address"),
                @SystemParameter(name = "mail.smtp.localport",
                                 description = "The local port number to bind to when "
                                         + "creating the SMTP socket.",
                                 defaultValue = "Defaults to the port number picked by the Socket class.",
                                 possibleParameters = "Any Integer"),
                @SystemParameter(name = "mail.smtp.ehlo",
                                 description = "If this parameter is set to 'false', you must not attempt to sign in" +
                                         " with the EHLO command.",
                                 defaultValue = "true",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.auth.login.disable",
                                 description = "If this is set to 'true', it is not allowed to use the 'AUTH LOGIN'" +
                                         " command.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.auth.plain.disable",
                                 description = "If this parameter is set to 'true', it is not allowed to use the " +
                                         "'AUTH PLAIN' command.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.auth.digest-md5.disable",
                                 description = "If this parameter is set to 'true', it is not allowed to use " +
                                         "the 'AUTH DIGEST-MD5' command.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.auth.ntlm.disable",
                                 description = "If this parameter is set to 'true', it is not allowed to use the " +
                                         "'AUTH NTLM' command",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.auth.ntlm.domain",
                                 description = "The NTLM authentication domain.",
                                 defaultValue = "None",
                                 possibleParameters = "The valid NTLM authentication domain name."),
                @SystemParameter(name = "mail.smtp.auth.ntlm.flags",
                                 description = "NTLM protocol-specific flags. "
                                         + "For more details, see http://curl.haxx.se/rfc/ntlm.html#theNtlmFlags.",
                                 defaultValue = "None",
                                 possibleParameters = "Valid NTLM protocol-specific flags."),
                @SystemParameter(name = "mail.smtp.dsn.notify",
                                 description = "The NOTIFY option to the RCPT command.",
                                 defaultValue = "None",
                                 possibleParameters = "Either 'NEVER', or a combination of 'SUCCESS', 'FAILURE', "
                                         + "and 'DELAY' (separated by commas)."),
                @SystemParameter(name = "mail.smtp.dsn.ret",
                                 description = "The 'RET' option to the 'MAIL' command.",
                                 defaultValue = "None",
                                 possibleParameters = "Either 'FULL' or 'HDRS'."),
                @SystemParameter(name = "mail.smtp.sendpartial",
                                 description = "If this parameter is set to 'true' and a message is addressed to " +
                                         "both valid and invalid addresses, the message is sent with a log that " +
                                         "reports the partial failure with a 'SendFailedException' error. If this " +
                                         "parameter is set to 'false' (which is default), the message is not sent to" +
                                         " any of the recipients when the recipient lists contain one or more invalid" +
                                         " addresses.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.sasl.enable",
                                 description = "If this parameter is set to 'true', the system attempts to use the " +
                                         "'javax.security.sasl' package to choose an authentication mechanism for the" +
                                         " login.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.sasl.mechanisms",
                                 description = "Enter a space or a comma-separated list of SASL mechanism names " +
                                         "that the system shouldt try to use.",
                                 defaultValue = "None",
                                 possibleParameters = ""),
                @SystemParameter(name = "mail.smtp.sasl.authorizationid",
                                 description = "The authorization ID to be used in the SASL authentication. "
                                         + "If no value is specified, the authentication ID (i.e., username) is used.",
                                 defaultValue = "username",
                                 possibleParameters = "Valid ID"),
                @SystemParameter(name = "mail.smtp.sasl.realm",
                                 description = "The realm to be used with the 'DIGEST-MD5' authentication.",
                                 defaultValue = "None",
                                 possibleParameters = ""),
                @SystemParameter(name = "mail.smtp.quitwait",
                                 description = "If this parameter is set to 'false', the 'QUIT' command is issued and" +
                                         " the connection is immediately closed. If this parameter is set to 'true' " +
                                         "(which is default), the transport waits for the response to the QUIT " +
                                         "command.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.reportsuccess",
                                 description = "If this parameter is set to 'true', the transport to includes"
                                         + " an 'SMTPAddressSucceededException' for each address to which the message" +
                                         " is successfully delivered.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.socketFactory",
                                 description = "If this parameter is set to a class that implements the " +
                                         "'javax.net.SocketFactory' interface, this class is used to create SMTP" +
                                         " sockets.",
                                 defaultValue = "None",
                                 possibleParameters = "Socket Factory"),
                @SystemParameter(name = "mail.smtp.socketFactory.class",
                                 description = "If this parameter is set, it specifies the name of a class that " +
                                         "implements the 'javax.net.SocketFactory interface'."
                                         + " This class is used to create SMTP sockets.",
                                 defaultValue = "None",
                                 possibleParameters = ""),
                @SystemParameter(name = "mail.smtp.socketFactory.fallback",
                                 description = "If this parameter is set to 'true', the failure to create a socket" +
                                         " using the specified socket factory class causes the socket to be created" +
                                         " using the 'java.net.Socket' class.",
                                 defaultValue = "true",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.socketFactory.port",
                                 description = "This specifies the port to connect to when using the specified " +
                                         "socket factory.",
                                 defaultValue = "25",
                                 possibleParameters = "Valid port number"),
                @SystemParameter(name = "mail.smtp.ssl.protocols",
                                 description = "This specifies the SSL protocols that need to be enabled for the" +
                                         " SSL connections.",
                                 defaultValue = "None",
                                 possibleParameters = "This parameter specifies a whitespace separated list of tokens "
                                         + "that are acceptable to the 'javax.net.ssl.SSLSocket.setEnabledProtocols'" +
                                         " method."),
                @SystemParameter(name = "mail.smtp.starttls.enable",
                                 description = "If this parameter is set to 'true', it is possible to issue the " +
                                         "'STARTTLS' command (if supported by the server) to switch the connection"
                                         + " to a TLS-protected connection before issuing any login commands.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.starttls.required",
                                 description = "If this parameter is set to 'true', it is required to use the " +
                                         "'STARTTLS' command."
                                         + " If the server does not support the 'STARTTLS' command, or if the " +
                                         "command fails, the connection method will fail.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.socks.host",
                                 description = "This specifies the host name of a SOCKS5 proxy server to be used for" +
                                         " the connections to the mail server.",
                                 defaultValue = "None",
                                 possibleParameters = ""),
                @SystemParameter(name = "mail.smtp.socks.port",
                                 description = "This specifies the port number for the SOCKS5 proxy server."
                                         + " This needs to be used only if the proxy server is not using the " +
                                         "standard port number 1080.",
                                 defaultValue = "1080",
                                 possibleParameters = "valid port number"),
                @SystemParameter(name = "mail.smtp.auth.ntlm.disable",
                                 description = "If this parameter is set to 'true', the AUTH NTLM command cannot " +
                                         "be issued.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.mailextension",
                                 description = "The extension string to be appended to the MAIL command.",
                                 defaultValue = "None",
                                 possibleParameters = ""),
                @SystemParameter(name = "mail.smtp.userset",
                                 description = "If this parameter is set to 'true', you should use the 'RSET' command" +
                                         " instead of the 'NOOP' command in the 'isConnected' method. In some " +
                                         "scenarios, 'sendmail' responds slowly after many 'NOOP' commands. This is" +
                                         " avoided by using 'RSET' instead.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false")
                }
)
public class EmailSink extends Sink {
    private static final Logger log = LogManager.getLogger(EmailSink.class);
    private Option optionSubject;
    private Option optionTo;
    private Option optionCc;
    private Option optionBcc;
    private Option optionContentType;
    private Map<String, String> initProperties = new HashMap<>();
    private Map<String, String> emailProperties = new HashMap<>();
    private ConfigReader configReader;
    private OptionHolder optionHolder;
    private List<String> attachments;
    private Option attachmentOption;

    /**
     * The initialization method for {@link Sink}, which will be called before other methods and validate
     * the all configuration and getting the intial values.
     *
     * @param streamDefinition  containing stream definition bind to the {@link Sink}
     * @param optionHolder      Option holder containing static and dynamic configuration related
     *                          to the {@link Sink}
     * @param configReader      to read the sink related system configuration.
     * @param siddhiAppContext  the context of the {@link io.siddhi.query.api.SiddhiApp} used to
     *                          get siddhi related utilty functions.
     */
    @Override
    protected StateFactory init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                                ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.configReader = configReader;
        this.optionHolder = optionHolder;
        //Server system properties starts with 'mail.smtp'.
        configReader.getAllConfigs().forEach((k, v)-> {
            if (k.startsWith("mail.smtp") || k.startsWith("mail.store")) {
                initProperties.put(k, v);
            }
        });
        validateAndGetRequiredParameters();
        return null;
    }

    /**
     * This method will be called before the processing method.
     * Intention to establish connection to publish event.
     *
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    public void connect() throws ConnectionUnavailableException {
        try {
            EmailConnectorFactory emailConnectorFactory = new EmailConnectorFactoryImpl();
            EmailClientConnectionPoolManager.initializeConnectionPool(emailConnectorFactory, initProperties);
        } catch (EmailConnectorException e) {
            if (e.getCause() instanceof MailConnectException) {
                if (e.getCause().getCause() instanceof ConnectException) {
                    throw new ConnectionUnavailableException("Error is encountered while connecting to the smtp"
                            + " server." +  e.getMessage(), e.getCause());
                } else {
                    throw new RuntimeException("Error is encountered while connecting to the smtp server." +
                            e.getMessage(), e.getCause());
                }
            } else {
                throw new RuntimeException("Error is encountered while connecting to the"
                        + " the smtp server." + e.getMessage(), e);
            }
        }
    }

    /**
     * This method will be called when events need to be published via this sink
     *
     * @param payload    payload of the event based on the supported event class exported by the extensions
     * @param dynamicOptions holds the dynamic options of this sink and Use this object to obtain dynamic options.
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions, State state)
            throws ConnectionUnavailableException {
        if (optionSubject != null) {
            String subject = optionSubject.getValue(dynamicOptions);
            emailProperties.put(EmailConstants.TRANSPORT_MAIL_HEADER_SUBJECT, subject);
        }
        if (optionTo != null) {
            String to = optionTo.getValue(dynamicOptions);
            emailProperties.put(EmailConstants.TRANSPORT_MAIL_HEADER_TO, to);
        }
        if (optionCc != null) {
            String cc = optionCc.getValue(dynamicOptions);
            emailProperties.put(EmailConstants.TRANSPORT_MAIL_HEADER_CC, cc);
        }
        if (optionBcc != null) {
            String bcc = optionBcc.getValue(dynamicOptions);
            emailProperties.put(EmailConstants.TRANSPORT_MAIL_HEADER_BCC, bcc);
        }
        if (optionContentType != null) {
            String contentType = optionContentType.getValue(dynamicOptions);
            emailProperties.put(EmailConstants.TRANSPORT_MAIL_HEADER_CONTENT_TYPE, contentType);
        }

        if ((attachmentOption != null) && (!attachmentOption.isStatic())) {
           attachments  =
                   Arrays.asList(attachmentOption.getValue(dynamicOptions).split(EmailConstants.COMMA_SEPERATOR));
        }

        EmailBaseMessage emailBaseMessage;
        if (attachmentOption != null) {
            emailBaseMessage = new EmailMultipartMessage(payload.toString(), attachments);
        } else {
            emailBaseMessage = new EmailTextMessage(payload.toString());
        }
        emailBaseMessage.setHeaders(emailProperties);
        GenericKeyedObjectPool objectPool = EmailClientConnectionPoolManager.getConnectionPool();
        if (objectPool != null) {
            EmailClientConnector connection = null;
            try {
                connection = (EmailClientConnector)
                        objectPool.borrowObject(EmailConstants.EMAIL_CLIENT_CONNECTION_POOL_ID);
                if (connection != null) {
                    connection.send(emailBaseMessage);
                }
            } catch (Exception e) {
                //calling super class logs the exception and retry
                if (e.getCause() instanceof MailConnectException) {
                    if (e.getCause().getCause() instanceof ConnectException) {
                        throw new ConnectionUnavailableException("Error is encountered while connecting the smtp"
                                + " server by the email ClientConnector.", e);
                    } else {
                        throw new RuntimeException("Error is encountered while sending the message by the email"
                                + " ClientConnector with properties: " + emailProperties.toString(), e);
                    }
                } else if (e.getCause() instanceof SMTPSendFailedException) {
                    throw new ConnectionUnavailableException("Error encountered while connecting " +
                            "to the mail server by the email client connector.", e);
                } else {
                    throw new RuntimeException("Error is encountered while sending the message by the email"
                            + " ClientConnector with properties: " + emailProperties.toString(), e);
                }
            } finally {
                if (connection != null) {
                    try {
                        objectPool.returnObject(EmailConstants.EMAIL_CLIENT_CONNECTION_POOL_ID, connection);
                    } catch (Exception e) {
                        log.error("Error in returning the email client connection object to the pool. " +
                                e.getMessage(), e);
                    }
                }
            }
        } else {
            log.error("Error in obtaining connection pool to publish emails to the server.");
        }
    }

    /**
     * Get the email parameters and validate them. If they are defined in correct way then they are put into the
     * email property map else throw SiddhiAppCreation exception.
     */
    private void validateAndGetRequiredParameters() {
        String username = optionHolder.validateAndGetStaticValue(EmailConstants.MAIL_PUBLISHER_USERNAME,
                configReader.readConfig(EmailConstants.MAIL_PUBLISHER_USERNAME, EmailConstants.EMPTY_STRING));
        if (username.isEmpty()) {
           throw new SiddhiAppCreationException(EmailConstants.MAIL_PUBLISHER_USERNAME + " is a mandatory parameter. "
                   + "It should be defined in either stream definition or deployment 'yaml' file.");
        }
        initProperties.put(EmailConstants.TRANSPORT_MAIL_PUBLISHER_USERNAME, username);

        String address = optionHolder.validateAndGetStaticValue(EmailConstants.MAIL_PUBLISHER_ADDRESS,
                configReader.readConfig(EmailConstants.MAIL_PUBLISHER_ADDRESS, EmailConstants.EMPTY_STRING));
        if (address.isEmpty()) {
            throw new SiddhiAppCreationException(EmailConstants.MAIL_PUBLISHER_ADDRESS + " is a mandatory parameter. "
                    + "It should be defined in either stream definition or deployment 'yaml' file.");
        }
        emailProperties.put(EmailConstants.TRANSPORT_MAIL_HEADER_FROM, address);

        String password = optionHolder.validateAndGetStaticValue(EmailConstants.MAIL_PUBLISHER_PASSWORD,
                configReader.readConfig(EmailConstants.MAIL_PUBLISHER_PASSWORD, ""));
        if (password.isEmpty()) {
            throw new SiddhiAppCreationException(EmailConstants.MAIL_PUBLISHER_PASSWORD + " is a mandatory parameter. "
                    + "It should be defined in either stream definition or deployment 'yaml' file.");
        }
        initProperties.put(EmailConstants.TRANSPORT_MAIL_PUBLISHER_PASSWORD, password);

        String host = optionHolder.validateAndGetStaticValue(EmailConstants.MAIL_PUBLISHER_HOST_NAME, configReader
                .readConfig(EmailConstants.MAIL_PUBLISHER_HOST_NAME, EmailConstants.MAIL_PUBLISHER_DEFAULT_HOST));
        initProperties.put(EmailConstants.TRANSPORT_MAIL_PUBLISHER_HOST_NAME, host);

        String sslEnable = optionHolder.validateAndGetStaticValue(EmailConstants.MAIL_PUBLISHER_SSL_ENABLE,
                configReader.readConfig(EmailConstants.MAIL_PUBLISHER_SSL_ENABLE,
                        EmailConstants.MAIL_PUBLISHER_DEFAULT_SSL_ENABLE));
        //validate string value of sslEnable is either true or false
        if (!(sslEnable.equals("true") || sslEnable.equals("false"))) {
            throw new SiddhiAppCreationException("Value of the " + EmailConstants.MAIL_PUBLISHER_SSL_ENABLE +
                    "should be either 'true' or 'false'.");
        }
        initProperties.put(EmailConstants.TRANSPORT_MAIL_PUBLISHER_SSL_ENABLE, sslEnable);

        String auth = optionHolder.validateAndGetStaticValue(EmailConstants.MAIL_PUBLISHER_AUTH,
                configReader.readConfig(EmailConstants.MAIL_PUBLISHER_AUTH,
                        EmailConstants.MAIL_PUBLISHER_DEFAULT_AUTH));
        //validate string value of auth enable is either true or false
        if (!(auth.equalsIgnoreCase("true") || auth.equalsIgnoreCase("false"))) {
            throw new SiddhiAppCreationException("Value of the " + EmailConstants.MAIL_PUBLISHER_AUTH +
                    "should be either 'true' or 'false'.");
        }
        initProperties.put(EmailConstants.TRANSPORT_MAIL_PUBLISHER_AUTH_ENABLE, auth);

        String port = optionHolder.validateAndGetStaticValue(EmailConstants.MAIL_PUBLISHER_PORT,
                configReader.readConfig(EmailConstants.MAIL_PUBLISHER_PORT, EmailConstants.EMPTY_STRING));
        //validate string value of auth enable is either true or false
        if (port.isEmpty()) {
            if (sslEnable.equalsIgnoreCase("true")) {
                port = EmailConstants.MAIL_PUBLISHER_DEFAULT_PORT;
            } else {
                throw new SiddhiAppCreationException("The default port: " + EmailConstants.MAIL_PUBLISHER_DEFAULT_PORT
                        + " can only be used if ssl enable.");
            }
        }
        initProperties.put(EmailConstants.TRANSPORT_MAIL_PUBLISHER_PORT, port);

        //Default we trust all the hosts (smtp servers). If user need to trust set of hosts then, it is required to
        //set 'ssl.trust' system property in deployment yaml under email sink configuration.
        String trust = configReader.readConfig(EmailConstants.MAIL_PUBLISHER_TRUST,
                EmailConstants.EMAIL_RECEIVER_DEFAULT_TRUST);
        initProperties.put(EmailConstants.MAIL_PUBLISHER_TRUST, trust);

        //to is a dynamic variable, if that option is not exist,
        // check whether default value for the 'to' is given in the configurations.
        if (!optionHolder.isOptionExists(EmailConstants.TO)) {
          String  to = configReader.readConfig(EmailConstants.TO, EmailConstants.EMPTY_STRING);
            if (to.isEmpty()) {
                throw new SiddhiAppCreationException(EmailConstants.TO + " is a mandatory parameter. "
                        + "It should be defined in either stream definition or deployment 'yaml' file.");
            } else {
                emailProperties.put(EmailConstants.TRANSPORT_MAIL_HEADER_TO, to);
            }
        } else {
            optionTo = optionHolder.validateAndGetOption(EmailConstants.TO);
        }

        //subject is a dynamic variable, if that option is not exist,
        // check whether default value for the subject is given in the configurations.
        if (!optionHolder.isOptionExists(EmailConstants.SUBJECT)) {
            String subject = configReader.readConfig(EmailConstants.SUBJECT, EmailConstants.EMPTY_STRING);
            if (subject.isEmpty()) {
                throw new SiddhiAppCreationException(EmailConstants.SUBJECT + " is a mandatory parameter. "
                        + "It should be defined in either stream definition or deployment 'yaml' file.");
            } else {
                emailProperties.put(EmailConstants.TRANSPORT_MAIL_HEADER_SUBJECT, subject);
            }
        } else {
            optionSubject = optionHolder.validateAndGetOption(EmailConstants.SUBJECT);
        }

        //cc is a dynamic variable, if that option is not exist,
        // check whether default value for the 'cc' is given in the configurations.
        if (!optionHolder.isOptionExists(EmailConstants.CC)) {
            String  cc = configReader.readConfig(EmailConstants.CC, EmailConstants.EMPTY_STRING);
            if (!cc.isEmpty()) {
                emailProperties.put(EmailConstants.TRANSPORT_MAIL_HEADER_CC, cc);
            }
        } else {
            optionCc = optionHolder.validateAndGetOption(EmailConstants.CC);
        }

        //bcc is a dynamic variable, if that option is not exist,
        // check whether default value for the 'bcc' is given in the configurations.
        if (!optionHolder.isOptionExists(EmailConstants.BCC)) {
            String  bcc = configReader.readConfig(EmailConstants.BCC, EmailConstants.EMPTY_STRING);
            if (!bcc.isEmpty()) {
                emailProperties.put(EmailConstants.TRANSPORT_MAIL_HEADER_BCC, bcc);
            }
        } else {
            optionBcc = optionHolder.validateAndGetOption(EmailConstants.BCC);
        }

        //content.type is a dynamic variable, if that option is not exist,
        // check whether default value for the 'content.type' is given in the configurations.
        if (!optionHolder.isOptionExists(EmailConstants.MAIL_PUBLISHER_CONTENT_TYPE)) {
            String contentType = configReader.readConfig(EmailConstants.MAIL_PUBLISHER_CONTENT_TYPE,
                    EmailConstants.EMPTY_STRING);
            if (!contentType.isEmpty()) {
                emailProperties.put(EmailConstants.TRANSPORT_MAIL_HEADER_CONTENT_TYPE, contentType);
            }
        } else {
            optionContentType = optionHolder.validateAndGetOption(EmailConstants.MAIL_PUBLISHER_CONTENT_TYPE);
        }

        String storeProtocol = optionHolder.validateAndGetStaticValue(
                EmailConstants.TRANSPORT_MAIL_PUBLISHER_STORE_PROTOCOL, configReader.readConfig(
                        EmailConstants.TRANSPORT_MAIL_PUBLISHER_STORE_PROTOCOL, EmailConstants.IMAP_STORE));
        initProperties.put(EmailConstants.TRANSPORT_MAIL_PUBLISHER_STORE_PROTOCOL, storeProtocol);

        if (optionHolder.isOptionExists(EmailConstants.ATTACHMENTS)) {
            attachmentOption = optionHolder.validateAndGetOption(EmailConstants.ATTACHMENTS);
            if (attachmentOption.isStatic()) {
                attachments = Arrays.asList(attachmentOption.getValue().split(EmailConstants.COMMA_SEPERATOR));
            }
        }
        String connectionPoolSize = optionHolder.validateAndGetStaticValue(EmailConstants.PUBLISHER_POOL_SIZE,
                configReader.readConfig(EmailConstants.PUBLISHER_POOL_SIZE, "1"));
        try {
            this.initProperties.put(EmailConstants.PUBLISHER_POOL_SIZE, connectionPoolSize);
        } catch (NumberFormatException e) {
            throw new SiddhiAppCreationException(EmailConstants.PUBLISHER_POOL_SIZE
                    + " parameter only excepts an Integer value.", e);
        }
    }

    /**
     * Called after all publishing is done, or when {@link ConnectionUnavailableException} is thrown
     * Implementation of this method should contain the steps needed to disconnect from the sink.
     */
    @Override public void disconnect() {
        EmailClientConnectionPoolManager.uninitializeConnectionPool();
    }

    /**
     * The method can be called when removing an event receiver.
     * The cleanups that has to be done when removing the receiver has to be done here.
     */
    @Override public void destroy() {
    }

    /**
     * Returns a list of supported dynamic options (that means for each event value of the option can change) by
     * the transport
     *
     * @return the list of supported dynamic option keys
     */
    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[]{EmailConstants.SUBJECT, EmailConstants.TO,
                EmailConstants.CC, EmailConstants.BCC, EmailConstants.ATTACHMENTS,
                EmailConstants.MAIL_PUBLISHER_CONTENT_TYPE};
    }

    /**
     * Returns the list of classes which this sink can consume.
     *
     * @return array of supported classes , if extension can support of any types of classes
     * then return empty array .
     */
    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class};
    }

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }
}
