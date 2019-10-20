/*
*  Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.wso2.extension.siddhi.io.email.sink.transport;

import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.wso2.extension.siddhi.io.email.util.EmailConstants;
import org.wso2.transport.email.contract.EmailConnectorFactory;
import org.wso2.transport.email.exception.EmailConnectorException;

import java.util.Map;

/**
 * This class is used hold the secure/non-secure connections for an Agent.
 */

public class EmailClientConnectionPoolManager {
    private static GenericKeyedObjectPool connectionPool;

    public static synchronized void initializeConnectionPool(EmailConnectorFactory emailConnectorFactory,
                                                Map<String, String> clientProperties) throws EmailConnectorException {
        EmailClientConnectionPoolFactory emailClientConnectionPoolFactory
                = new EmailClientConnectionPoolFactory(emailConnectorFactory, clientProperties);
        if (connectionPool == null) {
            int poolSize = Integer.parseInt(clientProperties.get(EmailConstants.PUBLISHER_POOL_SIZE));
            connectionPool = new GenericKeyedObjectPool();
            connectionPool.setFactory(emailClientConnectionPoolFactory);
            connectionPool.setMaxTotal(poolSize);
            connectionPool.setMaxActive(poolSize);
            connectionPool.setTestOnBorrow(true);
            connectionPool.setWhenExhaustedAction(GenericKeyedObjectPool.WHEN_EXHAUSTED_BLOCK);
        }
    }

    public static GenericKeyedObjectPool getConnectionPool() {
        return connectionPool;
    }

    public static void uninitializeConnectionPool() {
        connectionPool = null;
    }
}
