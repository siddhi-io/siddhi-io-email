package org.wso2.extension.siddhi.io.email.sink;

import com.icegreen.greenmail.user.UserException;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetupTest;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.config.InMemoryConfigManager;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import static org.testng.Assert.assertEquals;


/**
 * Class implementing test cases for the sink.
 */
public class EmailSinkTestCase {
    private static final Logger log = Logger.getLogger(EmailSinkTestCase.class);
    private static final String PASSWORD = "carbon123";
    private static final String USERNAME = "carbon";
    private static final String ADDRESS = "carbon@localhost";
    private static final String TO = "someone@localhost.com";
    private static final String HOST = "127.0.0.1";
    private GreenMail mailServer;

    @BeforeClass
    public void setUp() {
        mailServer = new GreenMail(ServerSetupTest.SMTP);
        mailServer.start();
    }

    @AfterClass
    public void tearDown() {
        mailServer.stop();
    }

    @Test
    public void getMails() throws IOException, MessagingException,
            UserException, InterruptedException {
        // setup user on the mail server
        mailServer.setUser(ADDRESS, USERNAME, PASSWORD);

        Map<String, String> masterConfigs = new HashMap<>();
        masterConfigs.put("sink.email.mail.smtp.port", "3025");
        SiddhiManager siddhiManager = new SiddhiManager();
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(masterConfigs, null);
        inMemoryConfigManager.generateConfigReader("sink", "email");
        siddhiManager.setConfigManager(inMemoryConfigManager);
        String streams = "" +
                "@App:name('TestSiddhiApp')"
                + "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='email', @map(type='json') , username='" + ADDRESS + "',"
                + "password= '" + PASSWORD + "', host ='" + HOST + "', ssl.enable = 'false',"
                + "auth = 'false',"
                + "subject='SP-{{symbol}}' , to='someone@localhost')"
                + "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});
        stockStream.send(new Object[]{"GOOGLE", 50f, 100L});
        stockStream.send(new Object[]{"REDHAT", 50f, 100L});
        Thread.sleep(5000);

        MimeMessage[] messages = mailServer.getReceivedMessages();
        assertEquals(messages.length, 4);
        MimeMessage m = messages[0];
        assertEquals(m.getSubject(), "SP-WSO2");

    }

}
