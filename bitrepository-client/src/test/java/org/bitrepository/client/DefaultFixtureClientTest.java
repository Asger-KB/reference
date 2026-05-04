/*
 * #%L
 * Bitrepository Protocol
 * 
 * $Id$
 * $HeadURL$
 * %%
 * Copyright (C) 2010 - 2011 The State and University Library, The Royal Library and The State Archives, Denmark
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
package org.bitrepository.client;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import org.bitrepository.client.conversation.mediator.CollectionBasedConversationMediator;
import org.bitrepository.client.conversation.mediator.ConversationMediator;
import org.bitrepository.common.settings.Settings;
import org.bitrepository.common.settings.TestSettingsProvider;
import org.bitrepository.common.utils.SettingsUtils;
import org.bitrepository.common.utils.TestFileHelper;
import org.bitrepository.protocol.MessageReceiverManager;
import org.bitrepository.protocol.activemq.ActiveMQMessageBus;
import org.bitrepository.protocol.bus.LocalActiveMQBroker;
import org.bitrepository.protocol.bus.MessageReceiver;
import org.bitrepository.protocol.fileexchange.HttpServerConfiguration;
import org.bitrepository.protocol.http.EmbeddedHttpServer;
import org.bitrepository.protocol.message.ClientTestMessageFactory;
import org.bitrepository.protocol.messagebus.MessageBus;
import org.bitrepository.protocol.messagebus.MessageBusManager;
import org.bitrepository.protocol.security.DummySecurityManager;
import org.bitrepository.protocol.security.SecurityManager;
import org.bitrepository.protocol.utils.TestWatcherExtension;
import org.bitrepository.settings.repositorysettings.MessageBusConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.LoggerFactory;
import org.testcontainers.activemq.ActiveMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.jms.JMSException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

/**
 * Contains the generic parts for tests integrating to the message bus.
 */
@Testcontainers(disabledWithoutDocker = true)
public abstract class DefaultFixtureClientTest {
    protected static final String DEFAULT_FILE_ID = ClientTestMessageFactory.FILE_ID_DEFAULT;
    public static LocalActiveMQBroker broker;
    public static EmbeddedHttpServer server;
    public static HttpServerConfiguration httpServerConfiguration;
    public static MessageBus messageBus;

    protected static MessageReceiver collectionReceiver;

    protected static String pillar1DestinationId;
    protected static MessageReceiver pillar1Receiver;
    protected static final String PILLAR1_ID = "Pillar1";

    protected static String pillar2DestinationId;
    protected static MessageReceiver pillar2Receiver;
    protected static final String PILLAR2_ID = "Pillar2";
    protected static String alarmDestinationID;
    protected static MessageReceiver alarmReceiver;
    protected static SecurityManager securityManager;
    protected static Settings settingsForCUT;
    protected static Settings settingsForTestClient;

    protected ConversationMediator conversationMediator;
    private MessageReceiverManager receiverManager;

    private String testMethodName;

    @Container
    ActiveMQContainer activemqContainer = new ActiveMQContainer("apache/activemq:5.17.7");

    private MessageBusConfiguration messageBusConfig;
    private String defaultFileId;
    private URL defaultFileUrl;
    protected String defaultDownloadFileAddress;
    protected String defaultUploadFileAddress;
    protected String collectionID;

    @RegisterExtension
    TestWatcherExtension testWatcher = new TestWatcherExtension();
    private String nonDefaultFileId;
    private String defaultAuditInformation;


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

        messageBus.setCollectionFilter(List.of());
        messageBus.setComponentFilter(List.of());
        receiverManager = new MessageReceiverManager(messageBus);

        alarmReceiver = addReceiver(new MessageReceiver(settingsForCUT.getAlarmDestination()));

        collectionReceiver = addReceiver(new MessageReceiver(settingsForCUT.getCollectionDestination()));

        pillar1DestinationId = "Pillar1_topic" + getTopicPostfix();
        pillar1Receiver = addReceiver(new MessageReceiver(pillar1DestinationId));

        pillar2DestinationId = "Pillar2_topic" + getTopicPostfix();
        pillar2Receiver = addReceiver(new MessageReceiver(pillar2DestinationId));

        receiverManager.startListeners();


    }

    @BeforeEach
    protected void initializeCUT() {
        renewConversationMediator();
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


    /**
     * Indicated whether an embedded http server should be started and used
     */
    public boolean useEmbeddedHttpServer() {
        return System.getProperty("useEmbeddedHttpServer", "false").equals("true");
    }


    /**
     * May be overridden by specific tests wishing to do stuff. Remember to call super if this is overridden.
     */
    protected void shutdownCUT() {
    }


    /**
     * Used for creating a new conversationMediator between tests, and for tests needing to use a differently configured
     * mediator.
     */
    protected void renewConversationMediator() {
        if (conversationMediator != null) {
            conversationMediator.shutdown();
        }
        conversationMediator = new CollectionBasedConversationMediator(settingsForCUT, securityManager);
    }

    @AfterEach
    public void shutdownConversationMediator() {
        if (conversationMediator != null) {
            conversationMediator.shutdown();
        }
        conversationMediator = null;
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
     * Hooks up the message bus.
     */
    protected void startMessageBus() {
        activemqContainer.start();
        while (!activemqContainer.isRunning()) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        messageBusConfig = new MessageBusConfiguration();
        messageBusConfig.setURL(activemqContainer.getBrokerUrl());
        messageBusConfig.setName(activemqContainer.getContainerName());
        settingsForTestClient.getRepositorySettings()
                             .getProtocolSettings()
                             .setMessageBusConfiguration(messageBusConfig);


        messageBus = new ActiveMQMessageBus(settingsForTestClient, securityManager);
        MessageBusManager.clear();
        MessageBusManager.injectCustomMessageBus(MessageBusManager.DEFAULT_MESSAGE_BUS, messageBus);
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


}
