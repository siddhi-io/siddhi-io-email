package io.siddhi.extension.io.email.sink;

import com.icegreen.greenmail.user.UserException;
import com.icegreen.greenmail.util.DummySSLSocketFactory;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetupTest;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.config.InMemoryConfigManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.security.Security;
import java.util.HashMap;
import java.util.Map;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Class implementing test cases for the sink.
 */
public class EmailSinkTestCase {
    private static final Logger log = LogManager.getLogger(EmailSinkTestCase.class);
    private static final String PASSWORD = "password";
    private static final String USERNAME = "abc";
    private static final String ADDRESS = "abc@localhost";
    private GreenMail mailServer;

    @AfterMethod
    public void tearDown() {
        mailServer.stop();
    }

    @Test(description = "Configure siddhi to email event publisher only using mandatory params")
    public void emailSinkTest1() throws IOException, MessagingException,
            UserException, InterruptedException {
        log.info("EmailSinkTest1 : Configure siddhi to email event publisher only using mandatory params.");
        // setup user on the mail server
        mailServer = new GreenMail(ServerSetupTest.SMTP);
        mailServer.start();
        mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("sink.email.port", "3025");
        masterConfigs.put("sink.email.host", "localhost");
        masterConfigs.put("sink.email.ssl.enable", "false");
        masterConfigs.put("sink.email.auth", "false");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("sink", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);
        String streams = "" +
                "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='email', @map(type='text') ,"
                + " username ='" + USERNAME + "',"
                + " address ='" + ADDRESS + "',"
                + " password= '" + PASSWORD + "',"
                + " subject='FooStream-{{symbol}}' ,"
                + " to='to@localhost')"
                + " define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});

        mailServer.waitForIncomingEmail(5000, 2);
        MimeMessage[] messages = mailServer.getReceivedMessages();
        assertEquals(messages.length, 2, "Send two messages.");
        assertEquals(messages[0].getSubject(), "FooStream-WSO2");
        assertEquals(InternetAddress.toString(messages[0].getRecipients(Message.RecipientType.TO)),
                "to@localhost");
        assertEquals(messages[0].getContent().toString(), "symbol:\"WSO2\",\r\n"
                + "price:55.6,\r\n" + "volume:100\r\n");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Configure siddhi for email event publisher with defining 'CC' & 'BCC'")
    public void emailSinkTest2() throws IOException, MessagingException,
            UserException, InterruptedException {
        // setup user on the mail server
        log.info("EmailSinkTest2 : Configure siddhi for email event publisher with defining 'CC' & 'BCC'.");
        mailServer = new GreenMail(ServerSetupTest.SMTP);
        mailServer.start();
        mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("sink.email.port", "3025");
        masterConfigs.put("sink.email.host", "localhost");
        masterConfigs.put("sink.email.ssl.enable", "false");
        masterConfigs.put("sink.email.auth", "false");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("sink", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);
        String streams = "" +
                "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='email', @map(type='text') ,"
                + " username ='" + USERNAME + "',"
                + " address ='" + ADDRESS + "',"
                + " password= '" + PASSWORD + "',"
                + " subject='FooStream' ,"
                + " to='to@localhost',"
                + " cc='cc@localhost',"
                + " bcc='kji@localhost')"
                + " define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});

        mailServer.waitForIncomingEmail(5000, 3);
        MimeMessage[] messages = mailServer.getReceivedMessages();
        //mail for all recipients are send to same INBOX.
        assertEquals(messages.length, 3, "Send one messages to each recipients."
                + " There are 3 recipients. Therefore total number of messages are 3 "
                + "in the localhost mail box.");
        assertEquals(InternetAddress.toString(messages[0].getRecipients(Message.RecipientType.TO)),
                "to@localhost");
        assertEquals(InternetAddress.toString(messages[0].getRecipients(Message.RecipientType.CC)),
                "cc@localhost");
        //BCC addresses are not contained in the message header in greenmail since some mail smtp provider not
        //allow to see the email bcc recipient list.
        assertEquals(messages[0].getSubject(), "FooStream");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Configure email event publisher with invalid host")
    public void emailSinkTest3() throws IOException, MessagingException,
            UserException, InterruptedException {
        log.info("EmailSinkTest3 : Configure email event publisher with invalid host.");
        // setup user on the mail server
        mailServer = new GreenMail(ServerSetupTest.SMTP);
        mailServer.start();
        mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("sink.email.port", "3025");
        masterConfigs.put("sink.email.ssl.enable", "false");
        masterConfigs.put("sink.email.auth", "false");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("sink", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);
        String streams = "" +
                "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='email', @map(type='text') ,"
                + " username ='" + USERNAME + "',"
                + " address ='" + ADDRESS + "',"
                + " password= '" + PASSWORD + "',"
                + " subject='FooStream' ,"
                + " to='to@localhost' ,"
                + " host='imap.localhost')"
                + " define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        try {
            siddhiAppRuntime.start();
        } catch (Exception e) {
            String exception = e.getMessage();
            Assert.assertTrue(exception.contains("Error is encountered while connecting to the smtp server"));
        }
    }

    @Test(description = "Configure email event publisher with invalid port")
    public void emailSinkTest4() throws IOException, MessagingException,
            UserException, InterruptedException {
        log.info("EmailSinkTest4 : Configure email event publisher with invalid port.");
        // setup user on the mail server
        mailServer = new GreenMail(ServerSetupTest.SMTP);
        mailServer.start();
        mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("sink.email.port", "221");
        masterConfigs.put("sink.email.ssl.enable", "false");
        masterConfigs.put("sink.email.auth", "false");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("sink", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);
        String streams = "" +
                "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='email', @map(type='text') ,"
                + " username ='" + USERNAME + "',"
                + " address ='" + ADDRESS + "',"
                + " password= '" + PASSWORD + "',"
                + " subject='FooStream' ,"
                + " to='to@localhost' ,"
                + " host='localhost')"
                + " define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        try {
            siddhiAppRuntime.start();
        } catch (Exception e) {
            String exception = e.getMessage();
            Assert.assertTrue(exception.contains("Error is encountered while connecting  to the smtp server"));
        }
    }


    @Test(description = "Configure siddhi to publish events to email via smtp with non-secure mode")
    public void emailSinkTest5() throws IOException, MessagingException,
            UserException, InterruptedException {
        log.info("EmailSinkTest5 : Configure siddhi to publish events to email via smtp with non-secure mode");
        // setup user on the mail server
        mailServer = new GreenMail(ServerSetupTest.SMTP);
        mailServer.start();
        mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("sink.email.port", "3025");
        masterConfigs.put("sink.email.host", "localhost");
        masterConfigs.put("sink.email.auth", "false");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("sink", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);
        String streams = "" +
                "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='email', @map(type='text') ,"
                + " username ='" + USERNAME + "',"
                + " address ='" + ADDRESS + "',"
                + " password= '" + PASSWORD + "',"
                + " ssl.enable= 'false',"
                + " subject='FooStream}' ,"
                + " to='to@localhost',"
                + " cc='cc@localhost',"
                + " bcc='bcc@localhost')"
                + " define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});

        mailServer.waitForIncomingEmail(5000, 3);
        MimeMessage[] messages = mailServer.getReceivedMessages();
        //mail for all recipients are send to same INBOX.
        assertTrue(messages.length > 0);
        assertTrue(messages[0].getSubject().contains("FooStream"));
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Configure email event publisher via imap host")
    public void emailSinkTest6() throws IOException, MessagingException,
            UserException, InterruptedException {
        log.info("EmailSinkTest6 : Configure email event publisher via imap host.");
        // setup user on the mail server
        mailServer = new GreenMail(ServerSetupTest.SMTP);
        mailServer.start();
        mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("sink.email.port", "3025");
        masterConfigs.put("sink.email.ssl.enable", "false");
        masterConfigs.put("sink.email.auth", "false");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("sink", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);
        String streams = "" +
                "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='email', @map(type='text') ,"
                + " username ='" + USERNAME + "',"
                + " address ='" + ADDRESS + "',"
                + " password= '" + PASSWORD + "',"
                + " subject='FooStream' ,"
                + " to='to@localhost' ,"
                + " host='imap.localhost')"
                + " define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        try {
            siddhiAppRuntime.start();
        } catch (Exception e) {
            String exception = e.getMessage();
            Assert.assertTrue(exception.contains("Error is encountered while connecting to the smtp server"));
        }
    }

    @Test(description = "Configure siddhi to publish events to email with defining multiple recipients in"
            + " to,cc,bcc list")
    public void emailSinkTest7() throws IOException, MessagingException,
            UserException, InterruptedException {
        log.info("EmailSinkTest7 : Configure siddhi to publish events to email with defining multiple recipients in"
                + " to,cc,bcc list");
        // setup user on the mail server
        mailServer = new GreenMail(ServerSetupTest.SMTP);
        mailServer.start();
        mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("sink.email.port", "3025");
        masterConfigs.put("sink.email.host", "localhost");
        masterConfigs.put("sink.email.ssl.enable", "false");
        masterConfigs.put("sink.email.auth", "false");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("sink", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);
        String streams = "" +
                "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='email', @map(type='text') ,"
                + " username ='" + USERNAME + "',"
                + " address ='" + ADDRESS + "',"
                + " password= '" + PASSWORD + "',"
                + " subject='FooStream' ,"
                + " to='to1@localhost,to2@localhost',"
                + " cc='cc1@localhost,cc2@localhost',"
                + " bcc='bcc1@localhost,bcc2localhost')"
                + " define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});

        mailServer.waitForIncomingEmail(5000, 6);
        MimeMessage[] messages = mailServer.getReceivedMessages();
        //mail for all recipients are send to same INBOX.
        assertEquals(messages.length, 6, "Send one messages to each recipients."
                + " There are 6 recipients. Therefore total number of messages are 6 "
                + "in the localhost mail box.");
        assertEquals(InternetAddress.toString(messages[0].getRecipients(Message.RecipientType.TO)),
                "to1@localhost, to2@localhost");
        assertEquals(InternetAddress.toString(messages[0].getRecipients(Message.RecipientType.CC)),
                "cc1@localhost, cc2@localhost");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Configure siddhi to email event publisher to publish the event mapped to xml")
    public void emailSinkTest8() throws IOException, MessagingException,
            UserException, InterruptedException {
        log.info("EmailSinkTest8 : Configure siddhi to email event publisher to publish the event mapped to xml.");
        // setup user on the mail server
        mailServer = new GreenMail(ServerSetupTest.SMTP);
        mailServer.start();
        mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("sink.email.port", "3025");
        masterConfigs.put("sink.email.host", "localhost");
        masterConfigs.put("sink.email.ssl.enable", "false");
        masterConfigs.put("sink.email.auth", "false");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("sink", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);
        String streams = "" +
                "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='email', @map(type='xml') ,"
                + " username ='" + USERNAME + "',"
                + " address ='" + ADDRESS + "',"
                + " password= '" + PASSWORD + "',"
                + " subject='FooStream' ,"
                + " to='to@localhost',"
                + " cc='cc@localhost',"
                + " bcc='bcc@localhost')"
                + " define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});

        mailServer.waitForIncomingEmail(5000, 3);
        MimeMessage[] messages = mailServer.getReceivedMessages();
        //mail for all recipients are send to same INBOX.
        assertEquals(messages.length, 3, "Send one messages to each recipients."
                + " There are 3 recipients. Therefore total number of messages are 3 "
                + "in the localhost mail box.");
        assertEquals(messages[0].getSubject(), "FooStream");
        assertEquals(messages[0].getContent().toString(),
                "<events><event><symbol>WSO2</symbol><price>55.6</price>"
                        + "<volume>100</volume></event></events>\r\n");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Configure siddhi to email event publisher to publish the event mapped to json")
    public void emailSinkTest9() throws IOException, MessagingException,
            UserException, InterruptedException {
        log.info("EmailSinkTest9 : Configure siddhi to email event publisher to publish the event mapped to json.");
        // setup user on the mail server
        mailServer = new GreenMail(ServerSetupTest.SMTP);
        mailServer.start();
        mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("sink.email.port", "3025");
        masterConfigs.put("sink.email.host", "localhost");
        masterConfigs.put("sink.email.ssl.enable", "false");
        masterConfigs.put("sink.email.auth", "false");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("sink", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);
        String streams = "" +
                "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='email', @map(type='json') ,"
                + " username ='" + USERNAME + "',"
                + " address ='" + ADDRESS + "',"
                + " password= '" + PASSWORD + "',"
                + " subject='FooStream' ,"
                + " to='to@localhost',"
                + " cc='cc@localhost',"
                + " bcc='bcc@localhost')"
                + " define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});

        mailServer.waitForIncomingEmail(5000, 3);
        MimeMessage[] messages = mailServer.getReceivedMessages();
        //mail for all recipients are send to same INBOX.
        assertEquals(messages.length, 3, "Send one messages to each recipients."
                + " There are 3 recipients. Therefore total number of messages are 3 "
                + "in the localhost mail box.");
        assertEquals(messages[0].getSubject(), "FooStream");
        assertEquals(messages[0].getContent().toString(),
                "{\"event\":{\"symbol\":\"WSO2\",\"price\":55.6,\"volume\":100}}\r\n");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Configure siddhi to publish events to email via smtp with securing as auth disabled")
    public void emailSinkTest10() throws IOException, MessagingException,
            UserException, InterruptedException {
        log.info("EmailSinkTest10 : "
                + "Configure siddhi to publish events to email via smtp with securing as auth disabled.");
        // setup user on the mail server
        Security.setProperty("ssl.SocketFactory.provider",
                DummySSLSocketFactory.class.getName());
        mailServer = new GreenMail(ServerSetupTest.SMTPS);
        mailServer.start();
        mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("sink.email.port", "3465");
        masterConfigs.put("sink.email.host", "localhost");
        masterConfigs.put("sink.email.auth", "false");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("sink", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);
        String streams = "" +
                "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='email', @map(type='text') ,"
                + " username ='" + USERNAME + "',"
                + " address ='" + ADDRESS + "',"
                + " password= '" + PASSWORD + "',"
                + " ssl.enable= 'true',"
                + " subject='FooStream}' ,"
                + " to='to@localhost',"
                + " cc='cc@localhost',"
                + " bcc='bcc@localhost')"
                + " define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});

        mailServer.waitForIncomingEmail(5000, 6);
        MimeMessage[] messages = mailServer.getReceivedMessages();
        //mail for all recipients are send to same INBOX.
        assertTrue(messages.length > 0);
        assertTrue(messages[0].getSubject().contains("FooStream"));
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Configure siddhi to publish events to email via smtp with securing and auth enable")
    public void emailSinkTest11() throws IOException, MessagingException,
            UserException, InterruptedException {
        log.info("EmailSinkTest11 : "
                + "Configure siddhi to publish events to email via smtp with securing as auth disabled.");
        // setup user on the mail server
        Security.setProperty("ssl.SocketFactory.provider",
                DummySSLSocketFactory.class.getName());
        mailServer = new GreenMail(ServerSetupTest.SMTPS);
        mailServer.start();
        mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("sink.email.port", "3465");
        masterConfigs.put("sink.email.host", "localhost");
        masterConfigs.put("sink.email.auth", "true");

        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("sink", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);
        String streams = "" +
                "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='email', @map(type='text') ,"
                + " username ='" + USERNAME + "',"
                + " address ='" + ADDRESS + "',"
                + " password= '" + PASSWORD + "',"
                + " ssl.enable= 'true',"
                + " subject='FooStream}' ,"
                + " to='to@localhost',"
                + " cc='cc@localhost',"
                + " bcc='bcc@localhost')"
                + " define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});

        mailServer.waitForIncomingEmail(5000, 3);
        MimeMessage[] messages = mailServer.getReceivedMessages();
        //mail for all recipients are send to same INBOX.
        assertTrue(messages.length > 0);
        assertTrue(messages[0].getSubject().contains("FooStream"));
        siddhiAppRuntime.shutdown();
    }
}
