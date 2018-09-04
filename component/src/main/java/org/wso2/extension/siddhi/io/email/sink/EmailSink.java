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

package org.wso2.extension.siddhi.io.email.sink;

import com.sun.mail.util.MailConnectException;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.email.util.EmailConstants;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.SystemParameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.Option;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
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
        description = "The email sink uses 'smtp' server to publish events via emails. It can be published events in"
                + " 'text', 'xml' or 'json' formats. The user can define email"
                + " sink parameters in either 'deployment yaml' file or stream definition."
                + " So that email source checks whether parameters are given in"
                + " stream definition or 'ymal' file respectively. If it is not given in both places,"
                + " then default values are taken for the optional parameters."
                + " If user need to configure server system parameters which are not given as options in"
                + " stream definition then it is needed to define them in 'yaml' file under email sink properties."
                + " (Refer link: https://javaee.github.io/javamail/SMTP-Transport to more information about"
                + " smtp server parameters). Further, some email account required to enable 'access to less secure"
                + " apps' option (for gmail account you can enable it via "
                + "https://myaccount.google.com/lesssecureapps).",
        parameters = {
                @Parameter(name = "username",
                           description = "Username of the email account which is used to send emails"
                                   + " (e.g: 'abc' is the username for abc@gmail.com).",
                           type = {DataType.STRING}),
                @Parameter(name = "address",
                           description = "Address of the email account which is used to send emails.",
                           type = {DataType.STRING}),
                @Parameter(name = "password",
                           description = "Password of the email account.",
                           type = {DataType.STRING}),
                @Parameter(name = "host",
                           description = "Host name of the smtp server "
                                   + "(e.g. host name for a gmail account : 'smtp.gmail.com'). The default value"
                                   + " 'smtp.gmail.com' is only valid if email account is a gmail account.",
                           type = {DataType.STRING},
                           optional = true,
                           defaultValue = "smtp.gmail.com"),
                @Parameter(name = "port",
                           description = "The port which is used to create the connection.",
                           type = {DataType.INT},
                           optional = true,
                           defaultValue = "'465' the default value is only valid is ssl enable"),
                @Parameter(name = "ssl.enable",
                           description = "Whether the connection should be established through"
                                   + " secure connection or not."
                                   + " The value can be either 'true' or 'false'. If it is 'true' then the connection "
                                   + "is establish through 493 port which is secure connection.",
                           type = {DataType.BOOL},
                           optional = true,
                           defaultValue = "true"),
                @Parameter(name = "auth",
                           description = "Whether to use AUTH command or not, while authenticating. If true,"
                                   + " then attempt to authenticate the user using the AUTH command.",
                           type = {DataType.BOOL},
                           optional = true,
                           defaultValue = "true"),
                @Parameter(name = "content.type",
                           description = "Content type can be either 'text/plain' or 'text/html'.",
                           type = {DataType.STRING},
                           optional = true,
                           defaultValue = "text/plain"),
                @Parameter(name = "subject",
                           description = "Subject of the mail which has to be send.",
                           type = {DataType.STRING},
                           dynamic = true),
                @Parameter(name = "to",
                           description = "Address of the 'to' recipients. If there are more than to recipients,"
                                   + " then addresses can be given as a comma separated list.",
                           type = {DataType.STRING},
                           dynamic = true),
                @Parameter(name = "cc",
                           description = "Address of the 'cc' recipients. If there are more than cc recipients,"
                                   + " then addresses can be given as a comma separated list.",
                           type = DataType.STRING,
                           optional = true,
                           defaultValue = "None"),
                @Parameter(name = "bcc",
                           description = "Address of the 'bcc' recipients. If there are more than bcc recipients,"
                                + " then addresses can be given as a comma separated list.",
                           type = DataType.STRING,
                           optional = true,
                           defaultValue = "None"),
                @Parameter(name = "attachments",
                        description = "File paths of the files that needs to be attached to the email.\n" +
                                "These paths should be absolute paths.\n" +
                                "They can be either directories or files\n. " +
                                "If it's a directory, all the files at its first level will be attached.",
                        type = DataType.STRING,
                        optional = true,
                        dynamic = true,
                        defaultValue = "None")
        },
        examples = {
                @Example(description = "Following example illustrates how to publish events using the email sink"
                        + "using mandatory parameters. As in the example, it publishes events come "
                        + "from the fooStream in json format via email sink "
                        + "to the given 'to' recipients."
                        + " The email is sent by the sender.account@gmail.com via secure connection.",

                        syntax =  "@sink(type='email', @map(type ='json'), "
                                + "username='sender.account', "
                                + "address='sender.account@gmail.com',"
                                + "password='account.password',"
                                + "subject='Alerts from Wso2 Stream Processor',"
                                + "to='{{email}}',"
                                + ")"
                                + "define stream fooStream (email string, loginId int, name string);"),

                @Example(description = "Following example illustrates how to configure the query parameters and "
                        + "system parameters in the deployment ymal file.\n "
                        + "Corresponding parameters need to be configure under name:'email' and namespace:'sink' as "
                        + "follows\n"
                        + "  siddhi: "
                        + "    extensions:\n"
                        + "      - extension:\n"
                        + "          name:'email'\n"
                        + "          namespace:'sink'\n"
                        + "          properties:\n"
                        + "            username: <sender's email username>\n"
                        + "            address: <sender's email address>\n"
                        + "            password: <sender's email password>\n"
                        + "\nAs in the example, it publishes events come"
                        + "from the fooStream in json format via email sink "
                        + "to the given 'to' recipients."
                        + " The email is sent by the sender.account@gmail.com via secure connection.",

                        syntax =  "@sink(type='email', @map(type ='json'), "
                                + "subject='Alerts from Wso2 Stream Processor',"
                                + "to='{{email}}',"
                                + ")"
                                + "define stream fooStream (email string, loginId int, name string);"),

                @Example(description = "Following example illustrates how to publish events using the email sink."
                        + " According to the example, it publishes events come from the fooStream in xml"
                        + " format via email sink as a text/html message"
                        + " to the given `to`,`cc` and `bcc` recipients using a secure connection. `name` in the"
                        + " `subject` attribute will be the value of the `name` parameter in the corresponding"
                        + " output event",

                        syntax =  "@sink(type='email', @map(type ='json'), "
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
                                + "define stream fooStream (name string, age int, country string);"),

                @Example(description = "Following example illustrates how to publish events using the"
                        + " email sink. Here files are also attached to the email."
                        + " According to the example, it publishes events come from the fooStream in xml"
                        + " format via email sink as a text/html message"
                        + " to the given `to`,`cc` and `bcc` recipients using a secure connection. `name` in the"
                        + " `subject` attribute will be the value of the `name` parameter in the corresponding"
                        + " output event.\n"
                        + " Also to the same email message, the local file(s) related to the path received for the "
                        + "attribute attachments is/are attached.",

                        syntax =  "@sink(type='email', @map(type ='json'), "
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
                                + "define stream fooStream (name string, age int, country string, attachments string)"
                                + ";"),
        },
        systemParameter = {
                @SystemParameter(name = "mail.smtp.ssl.trust",
                                 description = "If set, and a socket factory hasn't been specified, enables use of a "
                                         + "MailSSLSocketFactory. If set to \"*\", all hosts are trusted. If set to a"
                                         + " whitespace separated list of hosts, those hosts are trusted. Otherwise, "
                                         + "trust depends on the certificate the server presents.",
                                 defaultValue = "*",
                                 possibleParameters = "String"),
                @SystemParameter(name = "mail.smtp.connectiontimeout",
                                 description = "Socket connection timeout value in milliseconds. ",
                                 defaultValue = "infinite timeout",
                                 possibleParameters = "Any Integer"),
                @SystemParameter(name = "mail.smtp.timeout",
                                 description = "Socket I/O timeout value in milliseconds. ",
                                 defaultValue = "infinite timeout",
                                 possibleParameters = "Any Integer"),
                @SystemParameter(name = "mail.smtp.from",
                                 description = "Email address to use for SMTP MAIL command. "
                                         + "This sets the envelope return address.",
                                 defaultValue = "Defaults to msg.getFrom() "
                                         + "or InternetAddress.getLocalAddress().",
                                 possibleParameters = "Valid email address"),
                @SystemParameter(name = "mail.smtp.localport",
                                 description = "Local port number to bind to when "
                                         + "creating the SMTP socket.",
                                 defaultValue = "Defaults to the port number picked "
                                         + "by the Socket class.",
                                 possibleParameters = "Any Integer"),
                @SystemParameter(name = "mail.smtp.ehlo",
                                 description = "If false, do not attempt to sign on with the EHLO command.",
                                 defaultValue = "true",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.auth.login.disable",
                                 description = "If true, prevents use of the AUTH LOGIN command.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.auth.plain.disable",
                                 description = "If true, prevents use of the AUTH PLAIN command.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.auth.digest-md5.disable",
                                 description = "If true, prevents use of the AUTH DIGEST-MD5 command.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.auth.ntlm.disable",
                                 description = "If true, prevents use of the AUTH NTLM command",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.auth.ntlm.domain",
                                 description = "The NTLM authentication domain.",
                                 defaultValue = "None",
                                 possibleParameters = "Valid NTLM authentication domain name"),
                @SystemParameter(name = "mail.smtp.auth.ntlm.flags",
                                 description = "NTLM protocol-specific flags. "
                                         + "See http://curl.haxx.se/rfc/ntlm.html#theNtlmFlags for details.",
                                 defaultValue = "None",
                                 possibleParameters = "Valid NTLM protocol-specific flags."),
                @SystemParameter(name = "mail.smtp.dsn.notify",
                                 description = "The NOTIFY option to the RCPT command.",
                                 defaultValue = "None",
                                 possibleParameters = "Either NEVER, or some combination of SUCCESS, FAILURE, "
                                         + "and DELAY (separated by commas)."),
                @SystemParameter(name = "mail.smtp.dsn.ret",
                                 description = "The RET option to the MAIL command.",
                                 defaultValue = "None",
                                 possibleParameters = "Either FULL or HDRS."),
                @SystemParameter(name = "mail.smtp.sendpartial",
                                 description = "If set to true, and a message has some valid and "
                                         + "some invalid addresses, send the message anyway, reporting the partial"
                                         + " failure with a SendFailedException. If set to false (the default),"
                                         + " the message is not sent to any of the recipients"
                                         + " if there is an invalid recipient address.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.sasl.enable",
                                 description = "If set to true, attempt to use the javax.security."
                                         + "sasl package to choose an authentication mechanism for login.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.sasl.mechanisms",
                                 description = "A space or comma separated list of SASL mechanism names to try to use.",
                                 defaultValue = "None",
                                 possibleParameters = ""),
                @SystemParameter(name = "mail.smtp.sasl.authorizationid",
                                 description = "The authorization ID to use in the SASL authentication. "
                                         + "If not set, the authentication ID (user name) is used.",
                                 defaultValue = "username",
                                 possibleParameters = "Valid ID"),
                @SystemParameter(name = "mail.smtp.sasl.realm",
                                 description = "The realm to use with DIGEST-MD5 authentication.",
                                 defaultValue = "None",
                                 possibleParameters = ""),
                @SystemParameter(name = "mail.smtp.quitwait",
                                 description = "If set to false, the QUIT command is sent and the connection "
                                         + "is immediately closed. If set to true (the default),"
                                         + " causes the transport to wait for the response to the QUIT command.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.reportsuccess",
                                 description = "If set to true, causes the transport to include"
                                         + " an SMTPAddressSucceededException for each address that is successful.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.socketFactory",
                                 description = "If set to a class that implements the "
                                         + "javax.net.SocketFactory interface,"
                                         + " this class will be used to create SMTP sockets.",
                                 defaultValue = "None",
                                 possibleParameters = "Socket Factory"),
                @SystemParameter(name = "mail.smtp.socketFactory.class",
                                 description = "If set, specifies the name of a class that implements "
                                         + "the javax.net.SocketFactory interface."
                                         + " This class will be used to create SMTP sockets.",
                                 defaultValue = "None",
                                 possibleParameters = ""),
                @SystemParameter(name = "mail.smtp.socketFactory.fallback",
                                 description = "If set to true, failure to create a socket using"
                                         + " the specified socket factory class will cause the socket"
                                         + " to be created using the java.net.Socket class.",
                                 defaultValue = "true",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.socketFactory.port",
                                 description = "Specifies the port to connect to when using "
                                         + "the specified socket factory",
                                 defaultValue = "25",
                                 possibleParameters = "Valid port number"),
                @SystemParameter(name = "mail.smtp.ssl.protocols",
                                 description = "Specifies the SSL protocols that will be enabled for SSL connections.",
                                 defaultValue = "None",
                                 possibleParameters = "The property value is a whitespace separated list of tokens "
                                         + "acceptable to the javax.net.ssl.SSLSocket.setEnabledProtocols method."),
                @SystemParameter(name = "mail.smtp.starttls.enable",
                                 description = "If true, enables the use of the STARTTLS command"
                                         + " (if supported by the server) to switch the connection"
                                         + " to a TLS-protected connection before issuing any login commands.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.starttls.required",
                                 description = "If true, requires the use of the STARTTLS command."
                                         + " If the server doesn't support the STARTTLS command,"
                                         + " or the command fails, the connect method will fail.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.socks.host",
                                 description = "Specifies the host name of a SOCKS5 proxy server "
                                         + "that will be used for connections to the mail server.",
                                 defaultValue = "None",
                                 possibleParameters = ""),
                @SystemParameter(name = "mail.smtp.socks.port",
                                 description = "Specifies the port number for the SOCKS5 proxy server."
                                         + " This should only need to be used if the proxy server"
                                         + " is not using the standard port number of 1080.",
                                 defaultValue = "1080",
                                 possibleParameters = "valid port number"),
                @SystemParameter(name = "mail.smtp.auth.ntlm.disable",
                                 description = "If true, prevents use of the AUTH NTLM command",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),
                @SystemParameter(name = "mail.smtp.mailextension",
                                 description = "Extension string to append to the MAIL command.",
                                 defaultValue = "None",
                                 possibleParameters = ""),
                @SystemParameter(name = "mail.smtp.userset",
                                 description = "If set to true, use the RSET command instead of the NOOP command"
                                         + " in the isConnected method. In some cases sendmail will respond slowly"
                                         + " after many NOOP commands; use of RSET avoids this sendmail issue.",
                                 defaultValue = "false",
                                 possibleParameters = "true or false"),

                }
)
public class EmailSink extends Sink {
    private static final Logger log = Logger.getLogger(EmailSink.class);
    private EmailClientConnector emailClientConnector;
    private Option optionSubject;
    private Option optionTo;
    private Option optionCc;
    private Option optionBcc;
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
     * @param siddhiAppContext  the context of the {@link org.wso2.siddhi.query.api.SiddhiApp} used to
     *                          get siddhi related utilty functions.
     */
    @Override
    protected void init(StreamDefinition streamDefinition, OptionHolder optionHolder,
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
        EmailConnectorFactory emailConnectorFactory = new EmailConnectorFactoryImpl();
        try {
            emailClientConnector = emailConnectorFactory.createEmailClientConnector();
            emailClientConnector.init(initProperties);
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
    public void publish(Object payload, DynamicOptions dynamicOptions) throws ConnectionUnavailableException {
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
        try {
            emailClientConnector.send(emailBaseMessage);
        } catch (EmailConnectorException e) {
                //calling super class logs the exception and retry
                if (e.getCause() instanceof MailConnectException) {
                    if (e.getCause().getCause() instanceof ConnectException) {
                        throw new ConnectionUnavailableException("Error is encountered while connecting the smtp" 
                                + " server by the email ClientConnector.", e);
                    } else {
                        throw new RuntimeException("Error is encountered while sending the message by the email"
                                + " ClientConnector with properties: " + emailProperties.toString() , e);
                    }
                } else {
                    throw new RuntimeException("Error is encountered while sending the message by the email"
                            + " ClientConnector with properties: " + emailProperties.toString() , e);
                }
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
                    + "It should be defined in either stream definition or deployment 'ymal' file.");
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
                        + "It should be defined in either stream definition or deployment 'ymal' file.");
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
                        + "It should be defined in either stream definition or deployment 'ymal' file.");
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

        String storeProtocol = optionHolder.validateAndGetStaticValue(
                EmailConstants.TRANSPORT_MAIL_PUBLISHER_STORE_PROTOCOL, configReader.readConfig(
                        EmailConstants.TRANSPORT_MAIL_PUBLISHER_STORE_PROTOCOL, EmailConstants.IMAP_STORE));
        initProperties.put(EmailConstants.TRANSPORT_MAIL_PUBLISHER_STORE_PROTOCOL, storeProtocol);

        String contentType = optionHolder.validateAndGetStaticValue(EmailConstants.MAIL_PUBLISHER_CONTENT_TYPE,
                configReader.readConfig(EmailConstants.MAIL_PUBLISHER_CONTENT_TYPE,
                        EmailConstants.MAIL_PUBLISHER_DEFAULT_CONTENT_TYPE));
        emailProperties.put(EmailConstants.TRANSPORT_MAIL_HEADER_CONTENT_TYPE, contentType);

        if (optionHolder.isOptionExists(EmailConstants.ATTACHMENTS)) {
            attachmentOption = optionHolder.validateAndGetOption(EmailConstants.ATTACHMENTS);
            if (attachmentOption.isStatic()) {
                attachments = Arrays.asList(attachmentOption.getValue().split(EmailConstants.COMMA_SEPERATOR));
            }
        }
    }

    /**
     * Called after all publishing is done, or when {@link ConnectionUnavailableException} is thrown
     * Implementation of this method should contain the steps needed to disconnect from the sink.
     */
    @Override public void disconnect() {
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
        EmailConstants.CC, EmailConstants.BCC, EmailConstants.ATTACHMENTS};
    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for reconstructing the element to the same state on a different point of time
     * This is also used to identify the internal states and debuging
     *
     * @return all internal states should be return as an map with meaning full keys
     */
    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param restoreState is the stateful objects of the processing element as a map.
     *              This map will have the  same keys that is created upon calling currentState() method.
     */
    @Override
    public void restoreState(Map<String, Object> restoreState) {
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
}
