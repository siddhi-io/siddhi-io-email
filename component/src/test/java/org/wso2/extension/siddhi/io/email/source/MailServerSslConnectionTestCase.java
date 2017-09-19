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

import com.icegreen.greenmail.user.GreenMailUser;
import com.icegreen.greenmail.user.UserException;
import com.icegreen.greenmail.util.DummySSLSocketFactory;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetupTest;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.config.InMemoryConfigManager;

import java.security.Security;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

/**
 * Class implementing to test the email source with and without SSL connection.
 */
public class MailServerSslConnectionTestCase {
    private static final Logger log = Logger.getLogger(EmailSourceImapTestCase.class);
    private static final String PASSWORD = "password";
    private static final String USERNAME = "abc";
    private static final String ADDRESS = "abc@localhost";
    private static final String EMAIL_FROM = "someone@localhost";
    private static final String EMAIL_SUBJECT = "Test E-Mail";
    private static final String LOCALHOST = "localhost";
    private int waitTime = 50;
    private int timeout = 30000;
    private AtomicInteger eventCount;
    private List<String> receivedEventNameList;
    private GreenMail mailServer;

    @BeforeMethod
    public void setUp() {
        receivedEventNameList = new ArrayList<>(2);
        eventCount = new AtomicInteger(0);
           }
    @AfterMethod
    public void tearDown() {
        mailServer.stop();
    }

    @Test(groups = "email source", description = "Test scenario: Receive event trough"
            + " pop3 serve with secure connection.")
    public void siddhiEmailSourcePop3WithSSLEnable() throws MessagingException, UserException,
            InterruptedException {
        log.info("Test scenario: Receiving event trough pop3 serve with secure connection.");

        //setup the pop3s greenmail server and create a user
        Security.setProperty("ssl.SocketFactory.provider",
                DummySSLSocketFactory.class.getName());
        mailServer = new GreenMail(ServerSetupTest.POP3S);
        mailServer.start();
        GreenMailUser user = mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("source.email.folder", "INBOX");
        masterConfigs.put("source.email.search.term", "subject:Test");
        masterConfigs.put("source.email.polling.interval", "5");
        masterConfigs.put("source.email.action.after.processed", "DELETE");
        masterConfigs.put("source.email.folder.to.move", "");
        masterConfigs.put("source.email.content.type", "text/plain");
        masterConfigs.put("source.email.port", "3995");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("source", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);

        String streams = "" + "@App:name('TestSiddhiApp')"
                + "@source(type='email',"
                + "username= '" + USERNAME + "',"
                + "password = '" + PASSWORD + "',"
                + "store = 'pop3',"
                + "host = '" + LOCALHOST + "',"
                + "ssl.enable = 'true',"
                + " @map(type='xml'))"
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";
        String query = ""
                + "from FooStream "
                + "select * "
                + "insert into BarStream; ";
        String event1 =
                "<events>"
                        + "<event>"
                        + "<name>John</name>"
                        + "<age>100</age>"
                        + "<country>AUS</country>"
                        + "</event>"
                        + "</events>";
        String event2 =
                "<events>"
                        + "<event>"
                        + "<name>Mike</name>"
                        + "<age>20</age>"
                        + "<country>USA</country>"
                        + "</event>"
                        + "</events>";
        deliverMassage(event1, user);
        deliverMassage(event2, user);
        mailServer.waitForIncomingEmail(5000, 2);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override public void receive(Event[] inEvents) {
                EventPrinter.print(inEvents);
                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                    receivedEventNameList.add(event.getData(0).toString());
                }
            }
        });
        List<String> expected = new ArrayList<>(2);
        expected.add("John");
        expected.add("Mike");

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(eventCount.get(), 2 , "Event count should be equal to two");
        Assert.assertEquals(receivedEventNameList, expected, " Value of the name parameter"
                + " of the two events are John and Mike respectively.");
        Thread.sleep(500);
        siddhiAppRuntime.shutdown();
    }

    @Test(groups = "email source", description = "Test scenario: Receive event trough"
            + " pop3 serve without secure connection.")
    public void siddhiEmailSourcePpo3WithoutSSLEnable() throws MessagingException, UserException,
            InterruptedException {
        log.info("Test scenario: Receiving event trough pop3 serve without secure connection.");

        //setup the pop3s greenmail server and create a user
        mailServer = new GreenMail(ServerSetupTest.POP3);
        mailServer.start();
        GreenMailUser user = mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("source.email.folder", "INBOX");
        masterConfigs.put("source.email.search.term", "subject:Test");
        masterConfigs.put("source.email.polling.interval", "5");
        masterConfigs.put("source.email.action.after.processed", "DELETE");
        masterConfigs.put("source.email.folder.to.move", "");
        masterConfigs.put("source.email.content.type", "text/plain");
        masterConfigs.put("source.email.port", "3110");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("source", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);

        String streams = "" + "@App:name('TestSiddhiApp')"
                + "@source(type='email',"
                + "username= '" + USERNAME + "',"
                + "password = '" + PASSWORD + "',"
                + "store = 'pop3',"
                + "host = '" + LOCALHOST + "',"
                + "ssl.enable = 'false',"
                + " @map(type='xml'))"
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";
        String query = ""
                + "from FooStream "
                + "select * "
                + "insert into BarStream; ";
        String event1 =
                "<events>"
                        + "<event>"
                        + "<name>John</name>"
                        + "<age>100</age>"
                        + "<country>AUS</country>"
                        + "</event>"
                        + "</events>";
        String event2 =
                "<events>"
                        + "<event>"
                        + "<name>Mike</name>"
                        + "<age>20</age>"
                        + "<country>USA</country>"
                        + "</event>"
                        + "</events>";
        deliverMassage(event1, user);
        deliverMassage(event2, user);
        mailServer.waitForIncomingEmail(5000, 2);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override public void receive(Event[] inEvents) {
                EventPrinter.print(inEvents);
                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                    receivedEventNameList.add(event.getData(0).toString());
                }
            }
        });
        List<String> expected = new ArrayList<>(2);
        expected.add("John");
        expected.add("Mike");

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(eventCount.get(), 2 , "Event count should be equal to two");
        Assert.assertEquals(receivedEventNameList, expected, " Value of the name parameter"
                + " of the two events are John and Mike respectively.");
        Thread.sleep(500);
        siddhiAppRuntime.shutdown();
    }

    @Test(groups = "email source", description = "Test scenario: Receive event trough"
            + " imap serve without secure connection.")
    public void siddhiEmailSourceImapWithoutSSLEnableTest3() throws MessagingException, UserException,
            InterruptedException {

        log.info("Test scenario: Receiving event trough imap serve without secure connection.");
        // set up imap green mail server.
        mailServer = new GreenMail(ServerSetupTest.IMAP);
        mailServer.start();
        // create user on mail server
        GreenMailUser user = mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("source.email.folder", "INBOX");
        masterConfigs.put("source.email.search.term", "subject:Test");
        masterConfigs.put("source.email.polling.interval", "5");
        masterConfigs.put("source.email.action.after.processed", "SEEN");
        masterConfigs.put("source.email.folder.to.move", "");
        masterConfigs.put("source.email.content.type", "text/plain");
        masterConfigs.put("source.email.port", "3143");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("source", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);

        String streams = "" + "@App:name('TestSiddhiApp')"
                + "@source(type='email',"
                + "username= '" + USERNAME + "',"
                + "password = '" + PASSWORD + "',"
                + "store = 'imap',"
                + "host = '" + LOCALHOST + "',"
                + "ssl.enable = 'false',"
                + " @map(type='xml'))"
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";

        String query = ""
                + "from FooStream "
                + "select * "
                + "insert into BarStream; ";

        String event1 =
                "<events>"
                        + "<event>"
                        + "<name>John</name>"
                        + "<age>100</age>"
                        + "<country>AUS</country>"
                        + "</event>"
                        + "</events>";
        String event2 =
                "<events>"
                        + "<event>"
                        + "<name>Mike</name>"
                        + "<age>20</age>"
                        + "<country>USA</country>"
                        + "</event>"
                        + "</events>";
        deliverMassage(event1, user);
        deliverMassage(event2, user);
        mailServer.waitForIncomingEmail(5000, 2);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override public void receive(Event[] inEvents) {
                EventPrinter.print(inEvents);
                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                    receivedEventNameList.add(event.getData(0).toString());
                }
            }
        });
        List<String> expected = new ArrayList<>(2);
        expected.add("John");
        expected.add("Mike");

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(eventCount.intValue(), 2 , "Event count should be equal to two");
        Assert.assertEquals(receivedEventNameList, expected, " Value of the name parameter"
                + " of the two events are John and Mike respectively.");
        Thread.sleep(500);
        siddhiAppRuntime.shutdown();
    }

    private void deliverMassage(String event , GreenMailUser user) throws MessagingException, InterruptedException {
        MimeMessage message = new MimeMessage((Session) null);
        message.setFrom(new InternetAddress(EMAIL_FROM));
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(ADDRESS));
        message.setSubject(EMAIL_SUBJECT);
        message.setText(event);
        user.deliver(message);
        Thread.sleep(500);
    }
}
