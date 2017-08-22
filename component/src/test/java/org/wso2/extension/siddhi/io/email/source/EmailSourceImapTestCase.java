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
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
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
 * Class implementing Email source imap test cases.
 */
public class EmailSourceImapTestCase {
    private static final Logger log = Logger.getLogger(EmailSourceImapTestCase.class);
    private static final String PASSWORD = "analytics";
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
        Thread.sleep(500);
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
        Thread.sleep(500);

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
        masterConfigs.put("source.email.port", "3993");

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
        Thread.sleep(500);
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

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" + "@App:name('TestSiddhiApp')"
                + "@source(type='email', @map(type='xml'), "
                + "username= '" + USERNAME + "',"
                + "password = '" + PASSWORD + "',"
                + "store = 'imap',"
                + "port = '3993',"
                + "host = 'pop3.localhost',"
                + "ssl.enable = 'false')"
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

    private void deliverMassage(String event , GreenMailUser user) throws MessagingException {
        MimeMessage message = new MimeMessage((Session) null);
        message.setFrom(new InternetAddress(EMAIL_FROM));
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(ADDRESS));
        message.setSubject(EMAIL_SUBJECT);
        message.setText(event);
        user.deliver(message);
    }


}
