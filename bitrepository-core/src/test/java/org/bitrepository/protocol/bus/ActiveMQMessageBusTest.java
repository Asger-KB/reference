/*
 * #%L
 * Bitrepository Core
 * %%
 * Copyright (C) 2010 - 2015 The State and University Library, The Royal Library and The State Archives, Denmark
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

import org.bitrepository.bitrepositorymessages.DeleteFileRequest;
import org.bitrepository.bitrepositorymessages.IdentifyPillarsForDeleteFileRequest;
import org.bitrepository.bitrepositorymessages.IdentifyPillarsForDeleteFileResponse;
import org.bitrepository.protocol.activemq.ActiveMQMessageBus;
import org.bitrepository.protocol.message.ExampleMessageFactory;
import org.bitrepository.protocol.messagebus.MessageBusManager;
import org.bitrepository.settings.repositorysettings.MessageBusConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.activemq.ActiveMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.jms.Message;
import javax.jms.MessageListener;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.bitrepository.common.utils.AllureTestUtils.addDescription;
import static org.bitrepository.common.utils.AllureTestUtils.addStep;


/**
 * Runs the GeneralMessageBusTest using a LocalActiveMQBroker and a suitable
 * MessageBus based on TestContainers.  Regression tests utilized that uses Allure to generate reports.
 */
@Testcontainers(disabledWithoutDocker = true)
class ActiveMQMessageBusTest extends GeneralMessageBusTest {
    
    @Container
    ActiveMQContainer activemqContainer = new ActiveMQContainer("apache/activemq:5.17.7")
        .withLogConsumer(l -> System.out.print(l.getUtf8String()));
    
    private MessageBusConfiguration messageBusConfig;
    
    @Override
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
    
    @Override
    public boolean useEmbeddedMessageBus() {
        return false;
    }
    
    @Test
    @Tag("regressiontest")
    final void collectionFilterTest() throws Exception {
        addDescription("Test that message bus filters identify requests to other collection, eg. ignores these.");
        addStep("Send an identify request with a undefined 'Collection' header property, " +
                "eg. this identify requests should be handled by everybody.",
                "Verify that the message bus accepts this message.");
        String myCollectionID = "MyCollection";
        messageBus.setCollectionFilter(List.of(myCollectionID));
        String collectionDestination = settingsForTestClient.getCollectionDestination();
        
        try (RawMessagebus rawMessagebus = new RawMessagebus(messageBusConfig, securityManager)) {
            var identifyRequest = ExampleMessageFactory.createMessage(IdentifyPillarsForDeleteFileRequest.class);
            identifyRequest.setCollectionID(myCollectionID);
            javax.jms.Message msg = rawMessagebus.createMessage(identifyRequest);
            rawMessagebus.addHeader(msg,
                                    identifyRequest.getClass().getSimpleName(),
                                    identifyRequest.getReplyTo(),
                                    null,
                                    identifyRequest.getCorrelationID());
            
            rawMessagebus.sendMessage(collectionDestination, msg);
            collectionReceiver.waitForMessage(identifyRequest.getClass());
            
            addStep("Send an identify request with the 'Collection' header property set to my collection",
                    "Verify that the request bus accepts this message.");
            msg.setStringProperty(ActiveMQMessageBus.COLLECTION_ID_KEY, myCollectionID);
            rawMessagebus.sendMessage(collectionDestination, msg);
            collectionReceiver.waitForMessage(identifyRequest.getClass());
            
            addStep("Send an invalid message with the 'Receiver' header property set to another specific component",
                    "Verify that the message bus ignores this before parsing the message.");
            msg.setStringProperty(ActiveMQMessageBus.COLLECTION_ID_KEY, "OtherCollection");
            rawMessagebus.sendMessage(collectionDestination, msg);
            collectionReceiver.checkNoMessageIsReceived(identifyRequest.getClass());
        }
    }
    
    @Test
    @Tag("regressiontest")
    final void sendMessageToSpecificComponentTest() throws Exception {
        addDescription("Test that message bus correct uses the 'to' header property to indicated that the message " +
                       "is meant for a specific component");
        addStep("Send a message with the 'Recipient' parameter set to at specific component",
                "The MESSAGE_TO_KEY ");
        String receiverID = "specificReceiver";
        final BlockingQueue<Message> messageList = new LinkedBlockingDeque<>();
        try (RawMessagebus rawMessagebus = new RawMessagebus(messageBusConfig, securityManager)) {
            rawMessagebus.addListener(settingsForTestClient.getCollectionDestination(), new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    messageList.add(message);
                }
            });
            IdentifyPillarsForDeleteFileRequest messageToSend =
                ExampleMessageFactory.createMessage(IdentifyPillarsForDeleteFileRequest.class);
            messageToSend.setDestination(settingsForTestClient.getCollectionDestination());
            messageToSend.setTo(receiverID);
            messageBus.sendMessage(messageToSend);
            Message receivedMessage = messageList.poll(3, TimeUnit.SECONDS);
            Assertions.assertEquals(receiverID, receivedMessage.getStringProperty(ActiveMQMessageBus.MESSAGE_TO_KEY));
        }
    }
    
    @Test
    @Tag("regressiontest")
    final void toFilterTest() throws Exception {
        addDescription("Test that message bus filters identify requests to other components, eg. ignores these.");
        addStep("Send an identify request with a undefined 'To' header property, " +
                "eg. this identify requests should be handled by all components.",
                "Verify that the identify request bus accepts this identify request.");
        messageBus.setComponentFilter(Arrays.asList(settingsForTestClient.getComponentID()));
        String collectionDestination = settingsForTestClient.getCollectionDestination();
        
        try (RawMessagebus rawMessagebus = new RawMessagebus(messageBusConfig, securityManager)) {
            var identifyRequest = ExampleMessageFactory.createMessage(IdentifyPillarsForDeleteFileRequest.class);
            
            identifyRequest.setDestination(collectionDestination);
            javax.jms.Message msg = rawMessagebus.createMessage(identifyRequest);
            rawMessagebus.addHeader(msg,
                                    identifyRequest.getClass().getSimpleName(),
                                    identifyRequest.getReplyTo(),
                                    null,
                                    identifyRequest.getCorrelationID());
            rawMessagebus.sendMessage(collectionDestination, msg);
            var msg1 = collectionReceiver.waitForMessage(identifyRequest.getClass());
            
            addStep("Send an identify request with the 'To' header property set to this component",
                    "Verify that the identify request bus accepts this identify request.");
            msg.setStringProperty(ActiveMQMessageBus.MESSAGE_TO_KEY, settingsForTestClient.getComponentID());
            rawMessagebus.sendMessage(collectionDestination, msg);
            var msg2 = collectionReceiver.waitForMessage(identifyRequest.getClass());
            
            addStep("Send an invalid identify request with the 'To' header property set to another specific component",
                    "Verify that the identify request bus ignores this before parsing the identify request.");
            msg.setStringProperty(ActiveMQMessageBus.MESSAGE_TO_KEY, "OtherComponent");
            rawMessagebus.sendMessage(collectionDestination, msg);
            collectionReceiver.checkNoMessageIsReceived(identifyRequest.getClass());
            
            addStep("Send an identify response with the 'To' header property set to another component",
                    "Verify that the message bus accepts this message.");
            var identifyResponse = ExampleMessageFactory.createMessage(IdentifyPillarsForDeleteFileResponse.class);
            identifyRequest.setDestination(collectionDestination);
            javax.jms.Message response = rawMessagebus.createMessage(identifyResponse);
            rawMessagebus.addHeader(response,
                                    identifyResponse.getClass().getSimpleName(),
                                    identifyResponse.getReplyTo(),
                                    null,
                                    identifyRequest.getCorrelationID());
            response.setStringProperty(ActiveMQMessageBus.MESSAGE_TO_KEY, "OtherComponent");
            rawMessagebus.sendMessage(collectionDestination, response);
            var msg3 = collectionReceiver.waitForMessage(identifyResponse.getClass());
            
            addStep("Send an non-identify request with the 'To' header property set to another component",
                    "Verify that the message bus accepts this message.");
            var deleteFileRequest = ExampleMessageFactory.createMessage(DeleteFileRequest.class);
            deleteFileRequest.setDestination(collectionDestination);
            var deleteFileRequestMessage = rawMessagebus.createMessage(deleteFileRequest);
            rawMessagebus.addHeader(deleteFileRequestMessage,
                                    deleteFileRequest.getClass().getSimpleName(),
                                    deleteFileRequest.getReplyTo(),
                                    null,
                                    identifyRequest.getCorrelationID());
            response.setStringProperty(ActiveMQMessageBus.MESSAGE_TO_KEY, "OtherComponent");
            rawMessagebus.sendMessage(collectionDestination, deleteFileRequestMessage);
            var msg4 = collectionReceiver.waitForMessage(deleteFileRequest.getClass());
        }
    }
}