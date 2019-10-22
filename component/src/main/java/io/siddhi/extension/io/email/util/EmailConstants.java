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
package io.siddhi.extension.io.email.util;

/**
 * The class implementing Email Constants.
 */
public class EmailConstants {
    /**
     * Email sink configurations.
     */
    private EmailConstants(){};

    public static final String MAIL_PUBLISHER_USERNAME = "username";
    public static final String MAIL_PUBLISHER_ADDRESS  = "address";
    public static final String MAIL_PUBLISHER_PASSWORD = "password";
    public static final String MAIL_PUBLISHER_HOST_NAME = "host";
    public static final String MAIL_PUBLISHER_SSL_ENABLE = "ssl.enable";
    public static final String MAIL_PUBLISHER_TRUST = "mail.smtp.ssl.trust";
    public static final String MAIL_PUBLISHER_PORT = "port";
    public static final String MAIL_PUBLISHER_AUTH = "auth";
    public static final String MAIL_PUBLISHER_CONTENT_TYPE = "content.type";
    public static final String SUBJECT = "subject";
    public static final String TO = "to";
    public static final String BCC = "bcc";
    public static final String CC = "cc";
    public static final String ATTACHMENTS = "attachments";
    public static final String PUBLISHER_POOL_SIZE = "connection.pool.size";
    public static final String EMAIL_CLIENT_CONNECTION_POOL_ID = "email_client_connection_pool";

    /**
     * Default values for the email sink configurations.
     */
    public static final String MAIL_PUBLISHER_DEFAULT_HOST = "smtp.gmail.com";
    public static final String MAIL_PUBLISHER_DEFAULT_SSL_ENABLE = "true";
    public static final String MAIL_PUBLISHER_DEFAULT_PORT = "465";
    public static final String MAIL_PUBLISHER_DEFAULT_AUTH = "true";
    public static final String MAIL_PUBLISHER_DEFAULT_CONTENT_TYPE = "text/plain";
    public static final String MAIL_PUBLISHER_DEFAULY_TRUST = "*";

    /**
     * Required carbon transport properties to send the email.
     */
    public static final String TRANSPORT_MAIL_PUBLISHER_USERNAME = "username";
    public static final String TRANSPORT_MAIL_PUBLISHER_PASSWORD = "password";
    public static final String TRANSPORT_MAIL_PUBLISHER_HOST_NAME = "mail.smtp.host";
    public static final String TRANSPORT_MAIL_PUBLISHER_SSL_ENABLE = "mail.smtp.ssl.enable";
    public static final String TRANSPORT_MAIL_PUBLISHER_AUTH_ENABLE = "mail.smtp.auth";
    public static final String TRANSPORT_MAIL_PUBLISHER_PORT = "mail.smtp.port";
    public static final String TRANSPORT_MAIL_PUBLISHER_STORE_PROTOCOL = "mail.store.protocol";
    public static final String TRANSPORT_MAIL_HEADER_FROM = "From";
    public static final String TRANSPORT_MAIL_HEADER_TO = "To";
    public static final String TRANSPORT_MAIL_HEADER_CC = "Cc";
    public static final String TRANSPORT_MAIL_HEADER_BCC = "Bcc";
    public static final String TRANSPORT_MAIL_HEADER_SUBJECT = "Subject";
    public static final String TRANSPORT_MAIL_HEADER_CONTENT_TYPE = "Content-Type";

    /**
     * Email source configurations.
     */
    public static final String EMAIL_RECEIVER_USERNAME = "username";
    public static final String EMAIL_RECEIVER_PASSWORD = "password";
    public static final String EMAIL_RECEIVER_HOST = "host";
    public static final String EMAIL_RECEIVER_SSL_ENABLE = "ssl.enable";
    public static final String EMAIL_RECEIVER_PORT = "port";
    public static final String POLLING_INTERVAL = "polling.interval";
    public static final String EMAIL_RECEIVER_CONTENT_TYPE = "content.type";
    public static final String ACTION_AFTER_PROCESSED = "action.after.processed";
    public static final String STORE = "store";
    public static final String FOLDER = "folder";
    public static final String MOVE_TO_FOLDER = "move.to.folder";
    public static final String EMAIL_SEARCH_TERM = "search.term";
    public static final String EMAIL_RECEIVER_TRUST = "ssl.trust";

    /**
     * Default values for the email source configurations.
     */
    public static final String EMAIL_RECEIVER_DEFAULT_STORE = "imap";
    public static final String EMAIL_RECEIVER_DEFAULT_PORT = "993";
    public static final String EMAIL_RECEIVER_DEFAULT_SSL_ENABLE = "true";
    public static final String DEFAULT_FOLDER = "INBOX";
    public static final String DEFAULT_POLLING_INTERVAL = "600";
    public static final String EMAIL_RECEIVER_DEFAULT_CONTENT_TYPE = "text/plain";
    public static final String DEFAULT_AUTO_ACKNOWLEDGE = "false";
    public static final String EMAIL_RECEIVER_DEFAULT_TRUST = "*";

    /**
     * Required carbon transport properties to receive the email.
     */
    public static final String TRANSPORT_MAIL_RECEIVER_USERNAME = "username";
    public static final String TRANSPORT_MAIL_RECEIVER_PASSWORD = "password";
    public static final String TRANSPORT_MAIL_STORE = "storeType";
    public static final String TRANSPORT_MAIL_RECEIVER_HOST_NAME = "hostName";
    public static final String TRANSPORT_MAIL_POLLING_INTERVAL = "pollingInterval";
    public static final String TRANSPORT_MAIL_ACTION_AFTER_PROCESSED = "actionAfterProcessed";
    public static final String TRANSPORT_MAIL_FOLDER_NAME = "folderName";
    public static final String TRANSPORT_MAIL_AUTO_ACKNOWLEDGE = "autoAcknowledge";
    public static final String TRANSPORT_MAIL_MOVE_TO_FOLDER = "moveToFolder";
    public static final String TRANSPORT_MAIL_SEARCH_TERM = "searchTerm";
    public static final String TRANSPORT_MAIL_RECEIVER_CONTENT_TYPE = "contentType";

    /**
     * Represent empty string.
     */
    public static final String EMPTY_STRING = "";

    public static final String COMMA_SEPERATOR = ",";
    /**
     * Valid store types.
     */
    public static final String IMAP_STORE = "imap";
    public static final String POP3_STORE = "pop3";

    /**
     * Valid content types.
     */
     public static final String TEXT_PLAIN = "text/plain";
     public static final String TEXT_HTML = "text/html";

    /**
     * valid action for processed mail if store type is imap.
     */
    public enum ActionAfterProcessed {
        MOVE, SEEN, DELETE, FLAGGED, ANSWERED,
    }

    /**
     * valid keys for search term if store type is imap.
     */
    public enum SearchTermKeys {
        BCC, CC, SUBJECT, TO, FROM,
    }
}
