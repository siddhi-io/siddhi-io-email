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
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.security.Security;
import java.util.concurrent.atomic.AtomicInteger;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

/**
 * Class implementing to test the email source with and without SSL connection.
 */
public class SearchTermTestCase {
    private static final Logger log = LogManager.getLogger(SearchTermTestCase.class);
    private static final String PASSWORD = "password";
    private static final String USERNAME = "to";
    private static final String ADDRESS = "to@localhost";
    private static final String EMAIL_FROM = "abc@localhost";
    private static final String EMAIL_SUBJECT = "Test E-Mail";
    private static final String LOCALHOST = "localhost";
    private int waitTime = 5000;
    private int timeout = 500;
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

    @Test (description = "Configure siddhi to recieve events from email where value for"
            + " key subject set with special character (@)")
    public void siddhiEmailSourceTest1() throws MessagingException, UserException, InterruptedException {

        log.info("Test scenario: siddhi to recieve events from email where value for"
                + " key subject set with special character (@).");
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
                + "search.term = 'subject:da 4.0.0 @Release mail, from:abc, to:to, bcc:bcc, cc:cc' ,"
                + "content.type = 'text/plain',"
                + "action.after.processed = 'MOVE',"
                + "move.to.folder = 'ProcessedMail')"
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";

        String query = "from FooStream "
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
                        + "<age>100</age>"
                        + "<country>AUS</country>"
                        + "</event>"
                        + "</events>";
        String event3 =
                "<events>"
                        + "<event>"
                        + "<name>Ricky</name>"
                        + "<age>100</age>"
                        + "<country>AUS</country>"
                        + "</event>"
                        + "</events>";
        String event4 =
                "<events>"
                        + "<event>"
                        + "<name>Susi</name>"
                        + "<age>2</age>"
                        + "<country>AUS</country>"
                        + "</event>"
                        + "</events>";

        deliverMassage(event1, user, "da1 4.0.0 @Release mail", "abc@localhost",
                "to@localhost", "cc1@localhost", "bcc1@localhost");
        deliverMassage(event2, user, "da 4.0.0 @Release mail", "abc@localhost", "to@localhost",
                "cc@localhost", "bcc@localhost");
        deliverMassage(event3, user, "da1 4.0.0 @Release mail", "abc@localhost", "to@localhost",
                "cc@localhost", "bcc@localhost");
        deliverMassage(event4, user, "da2 4.0.0 @Release mail", "abc@localhost", "to@localhost",
                "cc3@localhost", "bcc1@localhost");
        mailServer.waitForIncomingEmail(5000, 12);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    eventCount.incrementAndGet();
                    Assert.assertEquals(event.getData(0), "Mike", "The condition which ");
                }

            }
        });

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertEquals(eventCount.intValue(), 1, "Event count should be equal to two.");
        Thread.sleep(500);
        siddhiAppRuntime.shutdown();
    }

    @Test (description = "Configure siddhi to recieve events from email where invalid search.term set in siddhi query")
    public void siddhiEmailSourceTest2() throws MessagingException, UserException, InterruptedException {

        log.info("Test scenario: siddhi to recieve events from email where invalid search.term set in siddhi query");
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
                + "search.term = 'subject:das, From:abc, To:abc, bcc:xyz:cc:xyz' ,"
                + "content.type = 'text/plain',"
                + "action.after.processed = 'MOVE',"
                + "move.to.folder = 'ProcessedMail')"
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";
        String query = "from FooStream "
                + "select * "
                + "insert into BarStream; ";
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        } catch (Exception e) {
            String exception = e.getMessage();
            Assert.assertTrue(exception.contains("search term 'subject:das, From:abc,"
                    + " To:abc, bcc:xyz:cc:xyz' is not in correct format."
                    + " It should be in 'key1:value1,key2:value2, ..., keyX:valueX format"));
        }
    }

    @Test (description = "Configure siddhi to recieve events from email where values set as"
            + " case insensitively in search.term")
    public void siddhiEmailSourceTest3() throws MessagingException, UserException, InterruptedException {

        log.info("Test scenario: Configure siddhi to recieve events from email"
                + " where values set as case insensitively in search.term");
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
                + "search.term = 'subject:DAS 4.0.0 @Release mail,from:ABC,to:TO,bcc:BCC,cc:CC',"
                + "content.type = 'text/plain',"
                + "action.after.processed = 'MOVE',"
                + "move.to.folder = 'ProcessedMail')"
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";

        String query = "from FooStream "
                + "select * "
                + "insert into BarStream; ";

        String event =
                "<events>"
                        + "<event>"
                        + "<name>Ricky</name>"
                        + "<age>100</age>"
                        + "<country>AUS</country>"
                        + "</event>"
                        + "</events>";

        deliverMassage(event, user, "das 4.0.0 @Release mail", "abc@localhost", "to@localhost",
                "cc@localhost", "bcc@localhost");
        mailServer.waitForIncomingEmail(5000, 3);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    eventCount.incrementAndGet();
                    Assert.assertEquals(event.getData(0), "Ricky");
                }

            }
        });

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertEquals(eventCount.intValue(), 1, "Event count should be equal to one.");
        Thread.sleep(500);
        siddhiAppRuntime.shutdown();
    }

    @Test (description = "Configure siddhi to recieve events from email where only one key"
            + " value defined in search.term")
    public void siddhiEmailSourceTest4() throws MessagingException, UserException, InterruptedException {

        log.info("Test scenario: Configure siddhi to recieve events from email where only one"
                + " key value defined in search.term");
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
                + "search.term = 'subject:DAS 4.0.0 @Release mail',"
                + "content.type = 'text/plain',"
                + "action.after.processed = 'MOVE',"
                + "move.to.folder = 'ProcessedMail')"
                + "define stream FooStream (name string, age int, country string); "
                + "define stream BarStream (name string, age int, country string); ";

        String query = "from FooStream "
                + "select * "
                + "insert into BarStream; ";

        String event =
                "<events>"
                        + "<event>"
                        + "<name>Ricky</name>"
                        + "<age>100</age>"
                        + "<country>AUS</country>"
                        + "</event>"
                        + "</events>";

        deliverMassage(event, user, "das 4.0.0 @Release mail");
        mailServer.waitForIncomingEmail(5000, 1);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    eventCount.incrementAndGet();
                    Assert.assertEquals(event.getData(0), "Ricky");
                }

            }
        });

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        Assert.assertEquals(eventCount.intValue(), 1, "Event count should be equal to one.");
        Thread.sleep(500);
        siddhiAppRuntime.shutdown();
    }

    // create an e-mail message using javax.mail
    // use greenmail to store the message
    private void deliverMassage(String event , GreenMailUser user, String subject)
            throws MessagingException {
        MimeMessage message = new MimeMessage((Session) null);
        message.setFrom(new InternetAddress(EMAIL_FROM));
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(ADDRESS));
        message.setSubject(subject);
        message.setText(event);
        user.deliver(message); }

    // create an e-mail message using javax.mail
    // use greenmail to store the message
    private void deliverMassage(String event , GreenMailUser user, String subject, String from, String to,
            String cc, String bcc) throws MessagingException {
        MimeMessage message = new MimeMessage((Session) null);
        message.setFrom(new InternetAddress(from));
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));
        message.addRecipient(Message.RecipientType.CC, new InternetAddress(cc));
        message.addRecipient(Message.RecipientType.BCC, new InternetAddress(bcc));
        message.setSubject(subject);
        message.setText(event);
        user.deliver(message); }

}
