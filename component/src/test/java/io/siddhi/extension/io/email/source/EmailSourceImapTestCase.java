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

import com.icegreen.greenmail.user.GreenMailUser;
import com.icegreen.greenmail.user.UserException;
import com.icegreen.greenmail.util.DummySSLSocketFactory;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetupTest;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.config.InMemoryConfigManager;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
 * Class implementing Email source imap test cases.
 */
public class EmailSourceImapTestCase {
    private static final Logger log = Logger.getLogger(EmailSourceImapTestCase.class);
    private static final String PASSWORD = "password";
    private static final String USERNAME = "abc";
    private static final String ADDRESS = "abc@localhost";
    private static final String EMAIL_FROM = "someone@localhost";
    private static final String EMAIL_SUBJECT = "Test E-Mail";
    private static final String LOCALHOST = "localhost";
    private int waitTime = 500;
    private int timeout = 5000;
    private AtomicInteger eventCount;
    private GreenMail mailServer;

    @BeforeMethod public void setUp() {
        eventCount = new AtomicInteger(0);
        Security.setProperty("ssl.SocketFactory.provider",
                DummySSLSocketFactory.class.getName());
        mailServer = new GreenMail(ServerSetupTest.IMAPS);
        mailServer.start();
    }

    @AfterMethod public void tearDown() {
        mailServer.stop();
    }

    @Test (description = "Test scenario: Configure siddhi to email event receiver"
            + " with all properties set in siddhi query correctly")
    public void siddhiEmailSourceTest1() throws MessagingException, UserException, InterruptedException {

        log.info("Test scenario: All properties set in siddhi query correctly.");
        //create user on mail server
        GreenMailUser user = mailServer.setUser(ADDRESS, USERNAME, PASSWORD);
        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" + "@App:name('TestSiddhiApp')"
                + "@source(type='email'," +  "@map(type='xml'),"
                + "username='" + USERNAME + "',"
                + "password='" + PASSWORD + "',"
                + "store = 'imap' ,"
                + "host = '" + LOCALHOST + "',"
                + "folder = 'INBOX',"
                + "ssl.enable = 'true' ,"
                + "port = '3993' ,"
                + "polling.interval = '5' ,"
                + "search.term = 'Subject: Test, from:someone' ,"
                + "content.type = 'text/plain',"
                + "action.after.processed = 'MOVE',"
                + "move.to.folder = 'ProcessedMail')"
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";

        String query = ""
                + "from FooStream "
                + "select * "
                + "insert into BarStream; ";

        String event =
                "<events>"
                        + "<event>"
                        + "<name>John</name>"
                        + "<age>100</age>"
                        + "<country>AUS</country>"
                        + "</event>"
                        + "<event>"
                        + "<name>Mike</name>"
                        + "<age>20</age>"
                        + "<country>USA</country>"
                        + "</event>"
                + "</events>";

        deliverMassage(event, user);
        mailServer.waitForIncomingEmail(5000, 1);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();

        List<String> received = new ArrayList<>(2);
        List<String> expected = new ArrayList<>(2);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override public void receive(Event[] events) {
                EventPrinter.print(events);
                    for (Event event : events) {
                        eventCount.incrementAndGet();
                        received.add(event.getData(0).toString());
                    }

            }
        });
        expected.add("John");
        expected.add("Mike");

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(eventCount.intValue(), 2, "Event count should be equal to two.");
        Assert.assertEquals(expected, received, " name parameter of received events are 'John' and"
                + "Mike respectively");
        Thread.sleep(500);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Test scenario: Configure siddhi to email event receiver only using mandatory params")
    public void siddhiEmailSourceTest2() throws MessagingException, UserException, InterruptedException {

        log.info("Test scenario: Configure email event receiver only using mandatory params.");
        // create user on mail server
        GreenMailUser user = mailServer.setUser(ADDRESS, USERNAME, PASSWORD);
        Map<String, String> masterConfigs = new HashMap<>();

        masterConfigs.put("source.mail.store", "imap");
        masterConfigs.put("source.email.host", LOCALHOST);
        masterConfigs.put("source.email.folder", "INBOX");
        masterConfigs.put("source.email.search.term", "subject:Test");
        masterConfigs.put("source.email.polling.interval", "5");
        masterConfigs.put("source.email.action.after.processed", "SEEN");
        masterConfigs.put("source.email.folder.to.move", "");
        masterConfigs.put("source.email.content.type", "text/plain");
        masterConfigs.put("source.email.ssl.enable", "true");
        masterConfigs.put("source.email.port", "3993");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("source", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);
        String streams = "" + "@App:name('TestSiddhiApp')"
                + "@source(type='email', @map(type='xml'), "
                + "username= '" + USERNAME + "',"
                + "password = '" + PASSWORD + "')"
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";

        String query = ""
                + "from FooStream "
                + "select * "
                + "insert into BarStream; ";

        String event =
                "<events>"
                        + "<event>"
                        + "<name>John</name>"
                        + "<age>100</age>"
                        + "<country>AUS</country>"
                        + "</event>"
                        + "<event>"
                        + "<name>Mike</name>"
                        + "<age>20</age>"
                        + "<country>USA</country>"
                        + "</event>"
               + "</events>";
        deliverMassage(event, user);
        mailServer.waitForIncomingEmail(5000, 1);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();

        List<String> received = new ArrayList<>(2);
        List<String> expected = new ArrayList<>(2);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    eventCount.incrementAndGet();
                    received.add(event.getData(0).toString());
                }

            }
        });
        expected.add("John");
        expected.add("Mike");

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(eventCount.intValue(), 2, "Event count should be equal to two.");
        Assert.assertEquals(expected, received, " name parameter of received events are 'John' and"
                + "Mike respectively");
        Thread.sleep(500);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Test scenario: Configure siddhi to receive events from mails via smtp")
    public void siddhiEmailSourceTest3() throws MessagingException, UserException, InterruptedException {

        log.info("Test scenario: Configure siddhi to receive events from mails via smtp.");
        // create user on mail server
        GreenMailUser user = mailServer.setUser(ADDRESS, USERNAME, PASSWORD);
        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("source.email.host", LOCALHOST);
        masterConfigs.put("source.email.folder", "INBOX");
        masterConfigs.put("source.email.search.term", "subject:Test");
        masterConfigs.put("source.email.polling.interval", "5");
        masterConfigs.put("source.email.action.after.processed", "SEEN");
        masterConfigs.put("source.email.folder.to.move", "");
        masterConfigs.put("source.email.content.type", "text/plain");
        masterConfigs.put("source.email.ssl.enable", "true");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("source", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);

        String streams = "" + "@App:name('TestSiddhiApp')"
                + "@source(type='email', @map(type='xml'), "
                + "username= '" + USERNAME + "',"
                + "password = '" + PASSWORD + "',"
                + "store = 'smtp',"
                + "port = '3993')"
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";

        String query = ""
                + "from FooStream "
                + "select * "
                + "insert into BarStream; ";

        String exception = null;

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        } catch (SiddhiAppCreationException e) {
            exception = e.getMessage();
        }
        Assert.assertTrue(exception.contains("store could be either imap or pop3. But found: smtp."),
                "pop3 or imap stores are used by email source to receive emails.");
    }

    @Test(description = "Test scenario: Configure siddhi to receive events from mails via imap with SSL.")
    public void siddhiEmailSourceTest4() throws MessagingException, UserException, InterruptedException {

        log.info("Test scenario: imap server with SSL.");
        // create user on mail server
        GreenMailUser user = mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("source.email.folder", "INBOX");
        masterConfigs.put("source.email.search.term", "subject:Test");
        masterConfigs.put("source.email.polling.interval", "5");
        masterConfigs.put("source.email.action.after.processed", "SEEN");
        masterConfigs.put("source.email.folder.to.move", "");
        masterConfigs.put("source.email.content.type", "text/plain");
        masterConfigs.put("source.email.mail.imap.port", "3993");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("source", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);

        String streams = "" + "@App:name('TestSiddhiApp')"
                + "@source(type='email', @map(type='xml'), "
                + "username= '" + USERNAME + "',"
                + "password = '" + PASSWORD + "',"
                + "store = 'imap',"
                + "host = '" + LOCALHOST + "',"
                + "ssl.enable = 'true')"
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";

        String query = ""
                + "from FooStream "
                + "select * "
                + "insert into BarStream; ";

        String event =
                "<events>"
                        + "<event>"
                        + "<name>John</name>"
                        + "<age>100</age>"
                        + "<country>AUS</country>"
                        + "</event>"
                        + "<event>"
                        + "<name>Mike</name>"
                        + "<age>20</age>"
                        + "<country>USA</country>"
                        + "</event>"
                        + "</events>";

        deliverMassage(event, user);
        mailServer.waitForIncomingEmail(5000, 1);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();

        List<String> received = new ArrayList<>(2);
        List<String> expected = new ArrayList<>(2);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    eventCount.incrementAndGet();
                    received.add(event.getData(0).toString());
                }

            }
        });
        expected.add("John");
        expected.add("Mike");

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(eventCount.intValue(), 2, "Event count should be equal to two.");
        Assert.assertEquals(expected, received, " name parameter of received events are 'John' and"
                + "Mike respectively");
        Thread.sleep(500);
        siddhiAppRuntime.shutdown();
    }


    @Test(description = "Test scenario: Configure email event receiver with invalid host")
    public void siddhiEmailSourceTest5() throws UserException, InterruptedException {

        log.info("Test scenario: Configure email event receiver with invalid host");
        // create user on mail server
        GreenMailUser user = mailServer.setUser(ADDRESS, USERNAME, PASSWORD);
        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("source.email.folder", "INBOX");
        masterConfigs.put("source.email.search.term", "subject:Test");
        masterConfigs.put("source.email.polling.interval", "5");
        masterConfigs.put("source.email.action.after.processed", "SEEN");
        masterConfigs.put("source.email.folder.to.move", "");
        masterConfigs.put("source.email.content.type", "text/plain");
        masterConfigs.put("source.email.mail.imap.port", "3993");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("source", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);

        String streams = "" + "@App:name('TestSiddhiApp')"
                + "@source(type='email', @map(type='xml'), "
                + "username= '" + USERNAME + "',"
                + "password = '" + PASSWORD + "',"
                + "store = 'imap',"
                + "port = '3993',"
                + "host = 'pop3.localhost',"
                + "ssl.enable = 'true')"
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";

        String query = ""
                + "from FooStream "
                + "select * "
                + "insert into BarStream; ";

        String exception = null;
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            siddhiAppRuntime.start();
        } catch (Exception e) {
            exception = e.getMessage();
            Assert.assertTrue(exception.contains("Error is encountered while connecting the Email Source"),
                    "Since pop3.localhost is a invalid host, it refuse to connect.");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(description = "Configure siddhi to recieve events from email where the email "
            + "has to have non-text content")
    public void siddhiEmailSourceTest6() throws MessagingException, UserException, InterruptedException {

        log.info("Test scenario: Configure siddhi to recieve events from email where the email"
                + " has to have non-text content");
        // create user on mail server
        GreenMailUser user = mailServer.setUser(ADDRESS, USERNAME, PASSWORD);
        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("source.email.host", LOCALHOST);
        masterConfigs.put("source.email.folder", "INBOX");
        masterConfigs.put("source.email.search.term", "subject:Test");
        masterConfigs.put("source.email.polling.interval", "5");
        masterConfigs.put("source.email.action.after.processed", "SEEN");
        masterConfigs.put("source.email.folder.to.move", "");
        masterConfigs.put("source.email.content.type", "text/json");
        masterConfigs.put("source.email.ssl.enable", "true");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("source", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);

        String streams = "" + "@App:name('TestSiddhiApp')"
                + "@source(type='email', @map(type='xml'), "
                + "username= '" + USERNAME + "',"
                + "password = '" + PASSWORD + "',"
                + "store = 'imap',"
                + "port = '3993')"
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";

        String query = ""
                + "from FooStream "
                + "select * "
                + "insert into BarStream; ";

        String exception = null;

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        } catch (SiddhiAppCreationException e) {
            exception = e.getMessage();
        }
        Assert.assertTrue(exception.contains("supported content types are 'text/html' and"
                + " 'text/plain' but found: text/json"));
    }


    private void deliverMassage(String event , GreenMailUser user) throws MessagingException {
        MimeMessage message = new MimeMessage((Session) null);
        message.setFrom(new InternetAddress(EMAIL_FROM));
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(ADDRESS));
        message.setSubject(EMAIL_SUBJECT);
        message.setText(event);
        user.deliver(message);
    }
}
