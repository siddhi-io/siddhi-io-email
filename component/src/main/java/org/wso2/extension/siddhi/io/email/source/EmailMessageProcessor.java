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
import org.wso2.carbon.messaging.CarbonCallback;
import org.wso2.carbon.messaging.CarbonMessage;
import org.wso2.carbon.messaging.CarbonMessageProcessor;
import org.wso2.carbon.messaging.ClientConnector;
import org.wso2.carbon.messaging.TextCarbonMessage;
import org.wso2.carbon.messaging.TransportSender;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

/**
 * The class implementing Email Message Processor.
 */
public class EmailMessageProcessor implements CarbonMessageProcessor {
    private static final Logger log = Logger.getLogger(EmailMessageProcessor.class);
    private SourceEventListener sourceEventListener;
    private String[] requiredProperties;
    private String contentType;

    public EmailMessageProcessor(SourceEventListener sourceEventListener, String[] requiredProperties,
            String contentType) {
        this.sourceEventListener = sourceEventListener;
        this.requiredProperties = requiredProperties.clone();
        this.contentType = contentType;
    }

    @Override
    public boolean receive(CarbonMessage carbonMessage, CarbonCallback carbonCallback) throws Exception {
        try {
            if (carbonMessage instanceof TextCarbonMessage) {
                String event = ((TextCarbonMessage) carbonMessage).getText();
                if (!event.isEmpty()) {
                    String[] transportProperties = getRequiredPropertyOrHeaderValues(carbonMessage);
                    sourceEventListener.onEvent(event, transportProperties);
                } else {
                    log.warn("Receive a message which satisfied the given criteria under"
                            + " the Search Term but in another"
                            + "content type: " + carbonMessage.getHeader("Content-Type")
                            + ". Therefore, skip the" + "message by further processing.");
                }
            } else {
                throw new SiddhiAppCreationException("Email source only support for the Text carbon message.");
            }
        } finally {
            carbonCallback.done(carbonMessage);
        }
        return true;
    }

    /**
     * Get required properties.
     * @param carbonMessage CarbonMessage received from the email server connector
     * @return String array which contain required property values.
     */
    private String[] getRequiredPropertyOrHeaderValues(CarbonMessage carbonMessage) {
        String[] values = new String[requiredProperties.length];
        int i = 0;
        for (String propertyKey : requiredProperties) {
            String headerValue = carbonMessage.getHeader(propertyKey);
            if (headerValue != null) {
                values[i++] = headerValue;
            } else {
                Object propertyValue = carbonMessage.getProperty(propertyKey);
                if (propertyValue != null) {
                    values[i++] = propertyValue.toString();
                } else {
                    log.error("Failed to find required transport property '" + propertyKey + "'.");
                }
            }
        }
        return values;
    }


    @Override public void setTransportSender(TransportSender transportSender) {

    }

    @Override public void setClientConnector(ClientConnector clientConnector) {

    }


    @Override public String getId() {
        return "email-message-processor";
    }

}
