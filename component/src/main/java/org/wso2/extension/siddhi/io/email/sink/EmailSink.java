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

import org.apache.log4j.Logger;
import org.wso2.carbon.messaging.CarbonMessage;
import org.wso2.carbon.messaging.ClientConnector;
import org.wso2.carbon.messaging.TextCarbonMessage;
import org.wso2.carbon.messaging.exceptions.ClientConnectorException;
import org.wso2.carbon.transport.email.sender.EmailClientConnector;
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

import java.util.HashMap;
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
                + " then default values are taken if defaults values are available."
                + " If user need to configure server system parameters which are not given as options in"
                + " stream definition then it is needed to define them in 'yaml' file under email sink properties."
                + " (Refer link: https://javaee.github.io/javamail/SMTP-Transport to more information about"
                + " smtp server parameters).",
        parameters = {
                @Parameter(name = "username",
                           description = "Username(address) of the email account which is used to send emails.",
                           type = {DataType.STRING}),
                @Parameter(name = "password",
                           description = "Password of the email account.",
                           type = {DataType.STRING}),
                @Parameter(name = "store",
                           //todo smtp
                           description = "Type of the store which is used to receive mails. It can be"
                                   + " either imap ,pop3 , imaps or pop3s.",
                           type = {DataType.STRING},
                           optional = true,
                           defaultValue = "smtp"),
                @Parameter(name = "host",
                           description = "Host name of the smtp server "
                                   + "(e.g. host name for a gmail account : 'smtp.gmail.com'). The default value"
                                   + " 'smtp.gmail.com' is only valid if email account is a gmail account.",
                           type = {DataType.STRING},
                           optional = true,
                           defaultValue = "smtp.gmail.com"),
                @Parameter(name = "ssl.enable",
                           description = "Whether the connection should be established through"
                                   + " secure connection or not."
                                   + " The value can be either 'true' or 'false'. If it is 'true' then the connection "
                                   + "is establish through 493 port which is secure connection.",
                           type = {DataType.STRING},
                           optional = true,
                           defaultValue = "true"),
                @Parameter(name = "auth",
                           description = "Whether to use AUTH command or not, while authenticating. If true,"
                                   + " then attempt to authenticate the user using the AUTH command.",
                           type = {DataType.STRING},
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
                           defaultValue = "None")

        },
        examples = {
                @Example(description = "Following example illustrates how to publish a event using email sink. As in "
                        + "the example, it publishes events come from the inputStream in json format through email to"
                        + " given 'to' and 'cc' recipients."
                        + "The email is sent through wso2@gmail.com email account via secure connection.",
                        syntax = "@sink(type='email', @map(type='json'), "
                                + "username='wso2@gmail.com', "
                                + "password='abc123',"
                                + "store='smtp'"
                                + "host='smtp.gmail.com',"
                                + "subject='Event from SP',"
                                + "to='wso2one@gmail.com ,wso2two@gmail.com',"
                                + "cc='wso2three@gmail.com"
                                + ")" +
                                "define stream inputStream (name string, age int, country string);"),
        },
        systemParameter = {
                @SystemParameter(name = "mail.smtp.port",
                                 description = "The SMTP server port to establish the connection.",
                                 defaultValue = "25",
                                 possibleParameters = "Any Integer"),
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
    private ClientConnector emailClientConnector;
    private Option optionSubject;
    private Option optionTo;
    private Map<String, String> emailProperties = new HashMap<>();
    private ConfigReader configReader;
    private OptionHolder optionHolder;

    @Override
    protected void init(StreamDefinition streamDefinition, OptionHolder optionHolder,
            ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.configReader = configReader;
        this.optionHolder = optionHolder;
        validateAndGetRequiredParameters();
        getServerSystemParameters();
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        emailClientConnector = new EmailClientConnector();
    }

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


        CarbonMessage textCarbonMessage = new TextCarbonMessage(payload.toString());

        try {
            emailClientConnector.send(textCarbonMessage, null, emailProperties);
        } catch (ClientConnectorException e) {
                //todo check exception and retry.
            throw new ConnectionUnavailableException("Error is encountered while sending the message by the email"
                    + " ClientConnector with properties: " + emailProperties.toString() , e);
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
        emailProperties.put(EmailConstants.TRANSPORT_MAIL_PUBLISHER_USERNAME, username);


        String password = optionHolder.validateAndGetStaticValue(EmailConstants.MAIL_PUBLISHER_PASSWORD,
                configReader.readConfig(EmailConstants.MAIL_PUBLISHER_PASSWORD, ""));
        if (password.isEmpty()) {
            throw new SiddhiAppCreationException(EmailConstants.MAIL_PUBLISHER_PASSWORD + " is a mandatory parameter. "
                    + "It should be defined in either stream definition or deployment 'ymal' file.");
        }
        emailProperties.put(EmailConstants.TRANSPORT_MAIL_PUBLISHER_PASSWORD, password);


        String host = optionHolder.validateAndGetStaticValue(EmailConstants.MAIL_PUBLISHER_HOST_NAME, configReader
                .readConfig(EmailConstants.MAIL_PUBLISHER_HOST_NAME, EmailConstants.MAIL_PUBLISHER_DEFAULT_HOST));
        emailProperties.put(EmailConstants.TRANSPORT_MAIL_PUBLISHER_HOST_NAME, host);


        String sslEnable = optionHolder.validateAndGetStaticValue(EmailConstants.MAIL_PUBLISHER_SSL_ENABLE,
                configReader.readConfig(EmailConstants.MAIL_PUBLISHER_SSL_ENABLE,
                        EmailConstants.MAIL_PUBLISHER_DEFAULT_SSL_ENABLE));
        //validate string value of sslEnable is either true or false
        if (!sslEnable.equals("true") && !sslEnable.equals("false")) {
            throw new SiddhiAppCreationException("Value of the " + EmailConstants.MAIL_PUBLISHER_SSL_ENABLE +
                    "should be either 'true' or 'false'.");
        }
        emailProperties.put(EmailConstants.TRANSPORT_MAIL_PUBLISHER_SSL_ENABLE, sslEnable);


        String auth = optionHolder.validateAndGetStaticValue(EmailConstants.MAIL_PUBLISHER_AUTH,
                configReader.readConfig(EmailConstants.MAIL_PUBLISHER_AUTH,
                        EmailConstants.MAIL_PUBLISHER_DEFAULT_AUTH));
        //validate string value of auth enable is either true or false
        //todo !(||)
        if (!auth.equalsIgnoreCase("true") && !auth.equalsIgnoreCase("false")) {
            throw new SiddhiAppCreationException("Value of the " + EmailConstants.MAIL_PUBLISHER_AUTH +
                    "should be either 'true' or 'false'.");
        }
        emailProperties.put(EmailConstants.TRANSPORT_MAIL_PUBLISHER_AUTH_ENABLE, auth);


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

        //todo put cc and bcc as dynamic

        String cc = optionHolder.validateAndGetStaticValue(EmailConstants.CC,
                configReader.readConfig(EmailConstants.CC, EmailConstants.EMPTY_STRING));
        if (!cc.isEmpty()) {
            emailProperties.put(EmailConstants.TRANSPORT_MAIL_HEADER_CC, cc);
        }


        String bcc = optionHolder.validateAndGetStaticValue(EmailConstants.BCC,
                configReader.readConfig(EmailConstants.BCC, EmailConstants.EMPTY_STRING));
        if (!bcc.isEmpty()) {
            emailProperties.put(EmailConstants.TRANSPORT_MAIL_HEADER_BCC, bcc);
        }

        String contentType = optionHolder.validateAndGetStaticValue(EmailConstants.MAIL_PUBLISHER_CONTENT_TYPE,
                configReader.readConfig(EmailConstants.MAIL_PUBLISHER_CONTENT_TYPE,
                        EmailConstants.MAIL_PUBLISHER_DEFAULT_CONTENT_TYPE));
        emailProperties.put(EmailConstants.TRANSPORT_MAIL_HEADER_CONTENT_TYPE, contentType);
    }

    /**
     * Get the server system properties
     */
    public void getServerSystemParameters() {
        Map<String, String> map = configReader.getAllConfigs();
        for (Map.Entry<String, String> parameter : map.entrySet()) {
            if (parameter.getKey().startsWith("mail.smtp")) {
                emailProperties.put(parameter.getKey(), parameter.getValue());
            }
        }
    }

    @Override public void disconnect() {

    }

    @Override public void destroy() {

    }

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[]{EmailConstants.SUBJECT, EmailConstants.TO};
    }


    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> map) {

    }

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class};
    }

}
