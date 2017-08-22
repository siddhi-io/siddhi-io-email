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
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetupTest;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.config.InMemoryConfigManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import javax.mail.Flags;
import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Store;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

/**
 * EmailSourceActionAfterProcessedTest case .
 */
public class EmailSourceActionAfterProcessedTestCase {
    private static final org.apache.log4j.Logger log = org.apache.log4j.Logger
            .getLogger(EmailSourceActionAfterProcessedTestCase.class);
    private static final String PASSWORD = "analytics123";
    private static final String USERNAME = "analytics";
    private static final String ADDRESS = "analytics@localhost";
    private static final String EMAIL_FROM = "someone@localhost";
    private static final String EMAIL_SUBJECT = "Test E-Mail";
    private static final String LOCALHOST = "localhost";
    private int waitTime = 500;
    private int timeout = 5000;
    AtomicInteger eventCount;
    private GreenMail mailServer;

    @BeforeMethod public void setUp() {
        eventCount = new AtomicInteger(0);
        mailServer = new GreenMail(ServerSetupTest.IMAP);
        mailServer.start();
    }

    @AfterMethod public void tearDown() {
        mailServer.stop();
    }

    @Test(description = "Test scenario: Configure siddhi to email event receiver when action.after.process is DELETE")
    public void siddhiEmailSourceActionAfterProcessedTest1() throws IOException, MessagingException,
            UserException, InterruptedException {

        log.info("Test1: email event receiver when action.after.process is DELETE.");

        //create a local mail server
        GreenMailUser user = mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();

        masterConfigs.put("source.email.search.term", "subject:Test");
        masterConfigs.put("source.email.folder", "INBOX");
        masterConfigs.put("source.email.polling.interval", "5");
        masterConfigs.put("source.email.content.type", "text/plain");
        masterConfigs.put("source.email.port", "3143");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("source", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);

        String streams = "" +
                "@App:name('TestSiddhiApp')"
                + "@source(type='email',@map(type='xml'), "
                + "username='" + USERNAME + "',"
                + "password='" + PASSWORD + "',"
                + "host = '" + LOCALHOST + "',"
                + "store ='imap',"
                + "ssl.enable = 'false',"
                + "action.after.processed='DELETE') "
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

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
        Thread.sleep(500);
        Message[] messages = getMessage("INBOX");
        Assert.assertEquals(messages.length, 1, "One message is in the INBOX");

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

        Message[] messagesAfterDelete = getMessage("INBOX");
        Assert.assertEquals(messagesAfterDelete.length, 0, "Message has been deleted after processing."
                + " Therefore, zero messages are found in 'INBOX'.");
        Thread.sleep(500);
        siddhiAppRuntime.shutdown();

    }


    @Test(description = "Configure siddhi to email event receiver when action.after.process is FLAGGED")
    public void siddhiEmailSourceActionAfterProcessedTest2() throws IOException, MessagingException,
            UserException, InterruptedException {
        log.info("Test 3: Email event receiver when action.after.process is FLAGGED");
        //create a local mail server
        GreenMailUser user = mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("source.email.search.term", "subject:Test");
        masterConfigs.put("source.email.folder", "INBOX");
        masterConfigs.put("source.email.polling.interval", "5");
        masterConfigs.put("source.email.content.type", "text/plain");
        masterConfigs.put("source.email.port", "3143");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("source", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);

        String streams = "" +
                "@App:name('TestSiddhiApp')"
                + "@source(type='email',@map(type='xml'), "
                + "username='" + USERNAME + "',"
                + "password='" + PASSWORD + "',"
                + "host = '" + LOCALHOST + "',"
                + "store ='imap',"
                + "ssl.enable = 'false',"
                + "action.after.processed='FLAGGED') "
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

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

        Message[] messagesAfterFlagged = getMessage("INBOX");
        Assert.assertEquals(messagesAfterFlagged.length, 1);
        Assert.assertTrue(messagesAfterFlagged[0].isSet(Flags.Flag.FLAGGED), "message is marked as FLAGGED");

        Thread.sleep(500);
        siddhiAppRuntime.shutdown();

    }

    @Test(description = "Configure siddhi to email event receiver when action.after.process is SEEN")
    public void siddhiEmailSourceActionAfterProcessedTest3()
            throws IOException, MessagingException, UserException, InterruptedException {

        log.info("Test 3: email event receiver when action.after.process is SEEN.");
        //create a local mail server
        GreenMailUser user = mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("source.email.search.term", "subject:Test");
        masterConfigs.put("source.email.folder", "INBOX");
        masterConfigs.put("source.email.polling.interval", "5");
        masterConfigs.put("source.email.content.type", "text/plain");
        masterConfigs.put("source.email.port", "3143");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("source", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);

        String streams = "" +
                "@App:name('TestSiddhiApp')"
                + "@source(type='email',@map(type='xml'), "
                + "username='" + USERNAME + "',"
                + "password='" + PASSWORD + "',"
                + "host = '" + LOCALHOST + "',"
                + "store ='imap',"
                + "ssl.enable = 'false',"
                + "action.after.processed='SEEN') "
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

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

        Message[] messagesAfterFlagged = getMessage("INBOX");
        Assert.assertEquals(messagesAfterFlagged.length, 1);
        Assert.assertTrue(messagesAfterFlagged[0].isSet(Flags.Flag.SEEN), "message is marked as SEEN.");

        Thread.sleep(500);
        siddhiAppRuntime.shutdown();

    }

    @Test(description = "Configure siddhi to email event receiver when action.after.process is ANSWERED")
    public void siddhiEmailSourceActionAfterProceesedTest4()
            throws IOException, MessagingException, UserException, InterruptedException {

        log.info("Test 4: email event receiver when action.after.process is ANSWERED.");
        //create a local mail server
        GreenMailUser user = mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("source.email.search.term", "subject:Test");
        masterConfigs.put("source.email.folder", "INBOX");
        masterConfigs.put("source.email.polling.interval", "5");
        masterConfigs.put("source.email.content.type", "text/plain");
        masterConfigs.put("source.email.port", "3143");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("source", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);

        String streams = "" +
                "@App:name('TestSiddhiApp')"
                + "@source(type='email',@map(type='xml'), "
                + "username='" + USERNAME + "',"
                + "password='" + PASSWORD + "',"
                + "host = '" + LOCALHOST + "',"
                + "store ='imap',"
                + "ssl.enable = 'false',"
                + "action.after.processed='ANSWERED') "
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

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

        Message[] messagesAfterFlagged = getMessage("INBOX");
        Assert.assertEquals(messagesAfterFlagged.length, 1);
        Assert.assertTrue(messagesAfterFlagged[0].isSet(Flags.Flag.ANSWERED), "message is marked as SEEN.");

        Thread.sleep(500);
        siddhiAppRuntime.shutdown();

    }

    @Test(description = "Configure siddhi to email event receiver when action.after.process is unsupported value")
    public void siddhiEmailSourceActionAfterProcessedTest5()
            throws IOException, MessagingException, UserException, InterruptedException {

        log.info("Test 5: email event receiver when action.after.process is STAR.");
        //create a local mail server
        GreenMailUser user = mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("source.email.port", "3143");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("source", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);

        String streams = "" +
                "@App:name('TestSiddhiApp')"
                + "@source(type='email',@map(type='xml'), "
                + "username='" + USERNAME + "',"
                + "password='" + PASSWORD + "',"
                + "host = '" + LOCALHOST + "',"
                + "store ='imap',"
                + "ssl.enable = 'false',"
                + "action.after.processed='STAR') "
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        String exception = null;

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        } catch (SiddhiAppCreationException e) {
            exception = e.getMessage();
        }

        Assert.assertTrue(exception.contains("action.after.processed could be 'MOVE, DELETE , SEEN, FLAGGED,"
                + " ANSWERED,'. But found: STAR"));
    }



    @Test(description = "Configure siddhi to email event receiver when action.after.process"
            + " is MOVE with existing folder.")
    public void siddhiEmailSourceActionAfterProcessedTest6()
            throws IOException, MessagingException, UserException, InterruptedException {

        log.info("Test 4: email event receiver when action.after.process is MOVE.");
        //create a local mail server
        GreenMailUser user = mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("source.email.search.term", "subject:Test");
        masterConfigs.put("source.email.folder", "INBOX");
        masterConfigs.put("source.email.polling.interval", "5");
        masterConfigs.put("source.email.content.type", "text/plain");
        masterConfigs.put("source.email.port", "3143");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("source", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);

        String streams = "" +
                "@App:name('TestSiddhiApp')"
                + "@source(type='email',@map(type='xml'), "
                + "username='" + USERNAME + "',"
                + "password='" + PASSWORD + "',"
                + "host = '" + LOCALHOST + "',"
                + "store ='imap',"
                + "ssl.enable = 'false',"
                + "action.after.processed='MOVE',"
                + "move.to.folder ='ProcessedMail' ) "
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

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
                        + "<age>100</age>"
                        + "<country>AUS</country>"
                        + "</event>"
                        + "</events>";

        deliverMassage(event1, user);
        deliverMassage(event2, user);

        Thread.sleep(1000);
        Message[] messages = getMessage("INBOX");
        Assert.assertEquals(messages.length, 2, "Two message is in the INBOX");

        createFolder("ProcessedMail");
        Assert.assertTrue(isFolderExist("ProcessedMail"), "folder has been created.");
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

        Message[] messagesInInboxAfterMove = getMessage("INBOX");
        Assert.assertEquals(messagesInInboxAfterMove.length, 0, "message is moved to another folder");

        Message[] messagesInProcessedMailFolder = getMessage("ProcessedMail");
        Assert.assertEquals(messagesInProcessedMailFolder.length, 2,
                "message is moved to another folder");

        Thread.sleep(500);
        siddhiAppRuntime.shutdown();

    }



    @Test(description = "Configure siddhi to email event receiver when action.after.process is MOVE"
            + " with non existing folder")
    public void siddhiEmailSourceActionAfterProcessedTest7()
            throws IOException, MessagingException, UserException, InterruptedException {

        log.info("Test 4: email event receiver when action.after.process is MOVE.");
        //create a local mail server
        GreenMailUser user = mailServer.setUser(ADDRESS, USERNAME, PASSWORD);


        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("source.email.search.term", "subject:Test");
        masterConfigs.put("source.email.folder", "INBOX");
        masterConfigs.put("source.email.polling.interval", "5");
        masterConfigs.put("source.email.content.type", "text/plain");
        masterConfigs.put("source.email.port", "3143");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("source", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);

        String streams = "" +
                "@App:name('TestSiddhiApp')"
                + "@source(type='email',@map(type='xml'), "
                + "username='" + USERNAME + "',"
                + "password='" + PASSWORD + "',"
                + "host = '" + LOCALHOST + "',"
                + "store ='imap',"
                + "ssl.enable = 'false',"
                + "action.after.processed='MOVE',"
                + "move.to.folder ='X-non-exist' ) "
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

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
                        + "<age>100</age>"
                        + "<country>AUS</country>"
                        + "</event>"
                        + "</events>";

        deliverMassage(event1, user);
        deliverMassage(event2, user);

        Thread.sleep(1000);
        Message[] messages = getMessage("INBOX");
        Assert.assertEquals(messages.length, 2, "Two message is in the INBOX");

        Assert.assertTrue(!isFolderExist("X-non-exist"));

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

        Message[] messagesInInboxAfterMove = getMessage("INBOX");
        Assert.assertEquals(messagesInInboxAfterMove.length, 0, "message is moved to another folder");

        Message[] messagesInProcessedMailFolder = getMessage("X-non-exist");
        Assert.assertEquals(messagesInProcessedMailFolder.length, 2,
                "message is moved to another folder");
        Thread.sleep(500);
        siddhiAppRuntime.shutdown();

    }

    @Test(description = "Configure siddhi to email event receiver when action.after.process is MOVE with 'INBOX'"
            + " as the folder")
    public void siddhiEmailSourceActionAfterProcessedTest8()
            throws IOException, MessagingException, UserException, InterruptedException {

        log.info("Test 4: email event receiver when action.after.process is MOVE.");
        final TestAppender appender = new TestAppender();
        final Logger log = Logger.getRootLogger();


        //create a local mail server
        GreenMailUser user = mailServer.setUser(ADDRESS, USERNAME, PASSWORD);
        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("source.email.search.term", "subject:Test");
        masterConfigs.put("source.email.folder", "INBOX");
        masterConfigs.put("source.email.polling.interval", "5");
        masterConfigs.put("source.email.content.type", "text/plain");
        masterConfigs.put("source.email.port", "3143");
        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("source", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);
        String streams = "" +
                "@App:name('TestSiddhiApp')"
                + "@source(type='email',@map(type='xml'), "
                + "username='" + USERNAME + "',"
                + "password='" + PASSWORD + "',"
                + "host = '" + LOCALHOST + "',"
                + "store ='imap',"
                + "ssl.enable = 'false',"
                + "action.after.processed='MOVE',"
                + "move.to.folder ='INBOX' ) "
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
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
                        + "<age>100</age>"
                        + "<country>AUS</country>"
                        + "</event>"
                        + "</events>";
        deliverMassage(event1, user);
        deliverMassage(event2, user);
        Thread.sleep(1000);
        Message[] messages = getMessage("INBOX");
        Assert.assertEquals(messages.length, 2, "Two message is in the INBOX");

       // Assert.assertTrue(!isFolderExist("X-non-exist"));

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
        Message[] messagesInInboxAfterMove = getMessage("INBOX");
        Assert.assertEquals(messagesInInboxAfterMove.length, 2, "message is moved to another folder");

        Thread.sleep(500);
        siddhiAppRuntime.shutdown();

    }


    // create an e-mail message using javax.mail ..
    // use greenmail to store the message
    private void deliverMassage(String event , GreenMailUser user) throws MessagingException {
        MimeMessage message = new MimeMessage((Session) null);
        message.setFrom(new InternetAddress(EMAIL_FROM));
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(ADDRESS));
        message.setSubject(EMAIL_SUBJECT);
        message.setText(event);
        user.deliver(message);
    }

    private Message[] getMessage(String folderName) throws MessagingException {
        Properties props = new Properties();
        props.put("mail.imap.port", "3143");
        Session session = Session.getInstance(props);
        Store store = session.getStore("imap");
        store.connect(LOCALHOST, USERNAME, PASSWORD);
        Folder folder = store.getFolder(folderName);
        folder.open(Folder.READ_ONLY);
        return folder.getMessages();
    }

    private void createFolder(String folderName) throws MessagingException {
        Properties props = new Properties();
        props.put("mail.imap.port", "3143");
        Session session = Session.getInstance(props);
        Store store = session.getStore("imap");
        store.connect(LOCALHOST, USERNAME, PASSWORD);
        Folder folder = store.getFolder(folderName);
        if (!folder.exists()) {
            folder.create(Folder.READ_WRITE);
        }
    }

    private boolean isFolderExist(String folderName) throws MessagingException {
        Properties props = new Properties();
        props.put("mail.imap.port", "3143");
        Session session = Session.getInstance(props);
        Store store = session.getStore("imap");
        store.connect(LOCALHOST, USERNAME, PASSWORD);
        Folder folder = store.getFolder(folderName);
        if (folder.exists()) {
            return true;
        }

        return false;
    }

    private class TestAppender extends AppenderSkeleton {
        private final List<LoggingEvent> log = new ArrayList<>();

        @Override
        public boolean requiresLayout() {
            return false;
        }

        @Override
        protected void append(final LoggingEvent loggingEvent) {
            log.add(loggingEvent);
        }

        @Override
        public void close() {
        }

        List<LoggingEvent> getLog() {
            return new ArrayList<LoggingEvent>(log);
        }
    }
}
