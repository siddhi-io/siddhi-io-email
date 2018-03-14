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
package org.wso2.extension.siddhi.io.email.source;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.transport.email.contract.EmailMessageListener;
import org.wso2.transport.email.contract.message.EmailBaseMessage;
import org.wso2.transport.email.contract.message.EmailTextMessage;

/**
 * The class implementing Email Source message listener to listen incoming email Messages.
 */
class EmailSourceMessageListener implements EmailMessageListener {
    private static final Logger log = Logger.getLogger(EmailSourceMessageListener.class);
    private SourceEventListener sourceEventListener;
    private String[] requiredProperties;
    private String contentType;

    public EmailSourceMessageListener(SourceEventListener sourceEventListener, String[] requiredProperties,
            String contentType) {
        this.sourceEventListener = sourceEventListener;
        this.requiredProperties = requiredProperties.clone();
        this.contentType = contentType;
    }

    @Override
    public void onMessage(EmailBaseMessage emailBaseMessage) {
        try {
            if (emailBaseMessage instanceof EmailTextMessage) {
                String event = ((EmailTextMessage) emailBaseMessage).getText();
                if (!event.isEmpty()) {
                    String[] transportProperties = getRequiredPropertyOrHeaderValues(emailBaseMessage);
                    sourceEventListener.onEvent(event, transportProperties);
                } else {
                    log.warn("Receive a message which satisfied the given criteria under"
                            + " the Search Term but in another"
                            + "content type: " + emailBaseMessage.getHeader("Content-Type")
                            + ". Therefore, skip the" + "message by further processing.");
                }
            } else {
                throw new SiddhiAppCreationException("Email source only support for the Text carbon message.");
            }
        } finally {
            emailBaseMessage.sendAck();
        }
    }

    /**
     * Get required properties.
     * @param emailBaseMessage emailBasedMessage received from the email server connector.
     * @return String array which contain required property values.
     */
    private String[] getRequiredPropertyOrHeaderValues(EmailBaseMessage emailBaseMessage) {
        String[] values = new String[requiredProperties.length];
        int i = 0;
        for (String propertyKey : requiredProperties) {
            String headerValue = emailBaseMessage.getHeader(propertyKey);
            if (headerValue != null) {
                values[i++] = headerValue;
            } else {
                Object propertyValue = emailBaseMessage.getProperty(propertyKey);
                if (propertyValue != null) {
                    values[i++] = propertyValue.toString();
                } else {
                    log.error("Failed to find required transport property '" + propertyKey + "'.");
                }
            }
        }
        return values;
    }

}
