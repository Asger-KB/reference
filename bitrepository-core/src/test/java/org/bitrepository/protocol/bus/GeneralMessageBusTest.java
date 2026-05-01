/*
 * #%L
 * Bitmagasin integrationstest
 *
 * $Id$
 * $HeadURL$
 * %%
 * Copyright (C) 2010 The State and University Library, The Royal Library and The State Archives, Denmark
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 2.1 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Lesser Public License for more details.
 *
 * You should have received a copy of the GNU General Lesser Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/lgpl-2.1.html>.
 * #L%
 */
package org.bitrepository.protocol.bus;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import org.bitrepository.SuiteInfoParameterResolver;
import org.bitrepository.bitrepositorymessages.AlarmMessage;
import org.bitrepository.bitrepositorymessages.IdentifyPillarsForGetFileRequest;
import org.bitrepository.bitrepositorymessages.Message;
import org.bitrepository.common.settings.Settings;
import org.bitrepository.common.settings.TestSettingsProvider;
import org.bitrepository.common.utils.SettingsUtils;
import org.bitrepository.common.utils.TestFileHelper;
import org.bitrepository.protocol.MessageContext;
import org.bitrepository.protocol.MessageReceiverManager;
import org.bitrepository.protocol.fileexchange.HttpServerConfiguration;
import org.bitrepository.protocol.http.EmbeddedHttpServer;
import org.bitrepository.protocol.message.ExampleMessageFactory;
import org.bitrepository.protocol.messagebus.MessageBus;
import org.bitrepository.protocol.messagebus.MessageBusManager;
import org.bitrepository.protocol.messagebus.MessageListener;
import org.bitrepository.protocol.messagebus.SimpleMessageBus;
import org.bitrepository.protocol.security.DummySecurityManager;
import org.bitrepository.protocol.security.SecurityManager;
import org.bitrepository.protocol.utils.TestWatcherExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.bitrepository.common.utils.AllureTestUtils.addDescription;
import static org.bitrepository.common.utils.AllureTestUtils.addStep;

/**
 * Class for testing the interface with the message bus.
 */
@ExtendWith(SuiteInfoParameterResolver.class)
abstract class GeneralMessageBusTest {
    /**
     * The time to wait when sending a message before it definitely should
     * have been consumed by a listener.
     */
    static final int TIME_FOR_WAIT = 2500;
    private final static int threadCount = 3;
    public static LocalActiveMQBroker broker;
    public static EmbeddedHttpServer server;
    public static HttpServerConfiguration httpServerConfiguration;
    public static MessageBus messageBus;
    
    protected static MessageReceiver collectionReceiver;
    protected static String alarmDestinationID;
    protected static MessageReceiver alarmReceiver;
    protected static SecurityManager securityManager;
    protected static Settings settingsForCUT;
    protected static Settings settingsForTestClient;
    
    protected static String collectionID;
    protected static String defaultFileId;
    protected static URL defaultFileUrl;
    protected static String defaultDownloadFileAddress;
    protected static String defaultUploadFileAddress;
    protected String nonDefaultFileId;
    protected String defaultAuditInformation;
    protected String testMethodName;
    
    private int count = 0;
    private final static String FINISH = "FINISH";
    private final BlockingQueue<String> finishQueue = new LinkedBlockingQueue<>(1);
    MultiMessageListener listener;
    
    private MessageReceiverManager receiverManager;
    
    @RegisterExtension
    TestWatcherExtension testWatcher = new TestWatcherExtension();
    
    
    @BeforeEach
    public void initializeSuite(TestInfo testInfo) {
        settingsForCUT = loadSettings(getComponentID());
        settingsForTestClient = loadSettings("TestSuiteInitialiser");
        makeUserSpecificSettings(settingsForCUT);
        makeUserSpecificSettings(settingsForTestClient);
        httpServerConfiguration =
            new HttpServerConfiguration(settingsForTestClient.getReferenceSettings().getFileExchangeSettings());
        collectionID = settingsForTestClient.getCollections().get(0).getID();
        
        securityManager = createSecurityManager();
        defaultFileId = "DefaultFile";
        try {
            defaultFileUrl = httpServerConfiguration.getURL(TestFileHelper.DEFAULT_FILE_ID);
            defaultDownloadFileAddress = defaultFileUrl.toExternalForm();
            defaultUploadFileAddress = defaultFileUrl.toExternalForm() + "-" + defaultFileId;
        } catch (MalformedURLException e) {
            throw new RuntimeException("Never happens");
        }
        startMessageBus();
        
        testMethodName = testInfo.getTestMethod().get().getName();
        setupSettings();
        nonDefaultFileId = TestFileHelper.createUniquePrefix(testMethodName);
        defaultAuditInformation = testMethodName;
        receiverManager = new MessageReceiverManager(messageBus);
        alarmReceiver = addReceiver(new MessageReceiver(settingsForCUT.getAlarmDestination()));
        collectionReceiver = addReceiver(new MessageReceiver(settingsForCUT.getCollectionDestination()));
        messageBus.setCollectionFilter(List.of());
        messageBus.setComponentFilter(List.of());
        receiverManager.startListeners();
    }
    
    @BeforeEach
    public void writeLogStatus() {
        if (System.getProperty("enableLogStatus", "false").equals("true")) {
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            StatusPrinter.print(lc);
        }
    }
    
    @AfterEach
    void tearDown() {
        if (receiverManager != null) {
            receiverManager.stopListeners();
        }
        if (testWatcher.isTestSuccessful()) {
            afterMethodVerification();
        }
        shutdownCUT();
        messageBus.setComponentFilter(List.of());
        messageBus.setCollectionFilter(List.of());
        teardownMessageBus();
        teardownHttpServer();
    }
    
    @Test
    @Tag("regressiontest")
    final void busActivityTest() throws Exception {
        addDescription("Tests whether it is possible to create a message listener, " +
                       "and then set it to listen to the topic. Then puts a message" +
                       "on the topic for the message listener to find, and" +
                       "tests whether it finds the correct message.");
        
        addStep("Send a message to the topic", "No exceptions should be thrown");
        AlarmMessage message = ExampleMessageFactory.createMessage(AlarmMessage.class);
        message.setDestination(alarmDestinationID);
        messageBus.sendMessage(message);
        
        addStep("Make sure both listeners received the message",
                "Both listeners received the message, and it is identical");
        alarmReceiver.waitForMessage(message.getClass());
    }
    
    @Test
    @Tag("regressiontest")
    final void twoListenersForTopicTest() throws Exception {
        addDescription("Verifies that two listeners on the same topic both receive the message");
        
        addStep("Make a connection to the message bus and add two listeners",
                "No exceptions should be thrown");
        MessageReceiver receiver1 = new MessageReceiver(alarmDestinationID);
        addReceiver(receiver1);
        messageBus.addListener(receiver1.getDestination(), receiver1.getMessageListener());
        MessageReceiver receiver2 = new MessageReceiver(alarmDestinationID);
        addReceiver(receiver2);
        messageBus.addListener(receiver2.getDestination(), receiver2.getMessageListener());
        
        addStep("Send a message to the topic", "No exceptions should be thrown");
        AlarmMessage message = ExampleMessageFactory.createMessage(AlarmMessage.class);
        message.setDestination(alarmDestinationID);
        messageBus.sendMessage(message);
        
        addStep("Make sure both listeners received the message",
                "Both listeners received the message, and it is identical");
        receiver1.waitForMessage(AlarmMessage.class);
        receiver2.waitForMessage(AlarmMessage.class);
    }
    
    @Test
    @Tag("specificationonly")
    final void messageBusFailoverTest() {
        addDescription("Verifies that we can switch to at second message bus " +
                       "in the middle of a conversation, if the connection is lost. " +
                       "We should also be able to resume the conversation on the new " +
                       "message bus");
    }
    
    @Test
    @Tag("specificationonly")
    final void messageBusReconnectTest() {
        addDescription("Test whether we are able to reconnect to the message " +
                       "bus if the connection is lost");
    }
    
      @Test
    @Tag("regressiontest")
    final void manyThreadsBeforeFinish() throws Exception {
        addDescription("Tests whether it is possible to start the handling of many threads simultaneously.");
        var idenfityRequest = ExampleMessageFactory.createMessage(IdentifyPillarsForGetFileRequest.class);
        listener = new MultiMessageListener();
        messageBus.addListener("BusActivityTest", listener);
        idenfityRequest.setDestination("BusActivityTest");
        
        addStep("Send one message for each listener",
                "When all have receiver, then they give respond on 'finishQueue'");
        for (int i = 0; i < threadCount; i++) {
            messageBus.sendMessage(idenfityRequest);
        }
        Assertions.assertEquals(FINISH, finishQueue.poll(TIME_FOR_WAIT, TimeUnit.MILLISECONDS));
        Assertions.assertEquals(threadCount, count);
    }
    
    
    protected MessageReceiver addReceiver(MessageReceiver receiver) {
        receiverManager.addReceiver(receiver);
        return receiver;
    }
    
    /**
     * May be used by specific tests for general verification when the test method has finished. Will only be run
     * if the test has passed (so far).
     */
    protected void afterMethodVerification() {
        receiverManager.checkNoMessagesRemainInReceivers();
    }
    
    /**
     * Purges all messages from the receivers.
     */
    protected void clearReceivers() {
        receiverManager.clearMessagesInReceivers();
    }
    
    /**
     * May be overridden by specific tests wishing to do stuff. Remember to call super if this is overridden.
     */
    protected void shutdownCUT() {
    }
    
    /**
     * Initializes the settings. Will postfix the alarm and collection topics with '-${user.name}
     */
    protected void setupSettings() {
        settingsForCUT = loadSettings(getComponentID());
        makeUserSpecificSettings(settingsForCUT);
        SettingsUtils.initialize(settingsForCUT);
        
        alarmDestinationID = settingsForCUT.getRepositorySettings().getProtocolSettings().getAlarmDestination();
        
        settingsForTestClient = loadSettings(testMethodName);
        makeUserSpecificSettings(settingsForTestClient);
    }
    
    protected Settings loadSettings(String componentID) {
        return TestSettingsProvider.reloadSettings(componentID);
    }
    
    private void makeUserSpecificSettings(Settings settings) {
        settings.getRepositorySettings().getProtocolSettings()
                .setCollectionDestination(settings.getCollectionDestination() + getTopicPostfix());
        settings.getRepositorySettings().getProtocolSettings()
                .setAlarmDestination(settings.getAlarmDestination() + getTopicPostfix());
    }
    
    
    /**
     * Indicated whether an embedded active MQ should be started and used
     */
    public boolean useEmbeddedMessageBus() {
        return System.getProperty("useEmbeddedMessageBus", "true").equals("true");
    }
    
    /**
     * Indicated whether an embedded http server should be started and used
     */
    public boolean useEmbeddedHttpServer() {
        return System.getProperty("useEmbeddedHttpServer", "false").equals("true");
    }
    
    /**
     * Hooks up the message bus.
     */
    protected void startMessageBus() {
        if (useEmbeddedMessageBus()) {
            if (messageBus == null) {
                messageBus = new SimpleMessageBus();
            }
            MessageBusManager.injectCustomMessageBus(MessageBusManager.DEFAULT_MESSAGE_BUS, messageBus);
            if (settingsForTestClient != null) {
                MessageBusManager.injectCustomMessageBus(settingsForTestClient.getComponentID(), messageBus);
            }
            if (settingsForCUT != null) {
                MessageBusManager.injectCustomMessageBus(settingsForCUT.getComponentID(), messageBus);
            }
        }
    }
    
    /**
     * Shutdown the message bus.
     */
    private void teardownMessageBus() {
        MessageBusManager.clear();
        if (messageBus != null) {
            try {
                messageBus.close();
                messageBus = null;
            } catch (JMSException e) {
            
            }
        }
        
        if (broker != null) {
            try {
                broker.stop();
                broker = null;
            } catch (Exception e) {
                // No reason to pollute the test output with this
            }
        }
    }
    
    /**
     * Shutdown the embedded http server if any.
     */
    protected void teardownHttpServer() {
        if (useEmbeddedHttpServer()) {
            server.stop();
        }
    }
    
    /**
     * Returns the postfix string to use when accessing user specific topics, which is the mechanism we use in the
     * bit repository tests.
     *
     * @return The string to postfix all topix names with.
     */
    protected String getTopicPostfix() {
        return "-" + System.getProperty("user.name");
    }
    
    protected String getComponentID() {
        return getClass().getSimpleName();
    }
    
    protected String createDate() {
        return Long.toString(System.currentTimeMillis());
    }
    
    protected SecurityManager createSecurityManager() {
        return new DummySecurityManager();
    }
    
    protected class MultiMessageListener implements MessageListener {
        private final BlockingQueue<String> queue = new LinkedBlockingQueue<>(threadCount);
        
        @Override
        public final void onMessage(Message message, MessageContext messageContext) {
            try {
                testIfFinished();
                Assertions.assertNotNull(queue.poll(TIME_FOR_WAIT, TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                Assertions.fail("Should not throw an exception: ", e);
            }
        }
        
        private void testIfFinished() throws InterruptedException {
            count++;
            if (count >= threadCount) {
                for (int i = 0; i < threadCount; i++) {
                    queue.put("Count '" + i + "'");
                }
                finishQueue.put(FINISH);
            }
        }
    }
}
