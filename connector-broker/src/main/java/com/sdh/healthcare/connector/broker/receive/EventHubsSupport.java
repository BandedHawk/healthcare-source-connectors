/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.messaging.eventhubs.CheckpointStore;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import java.util.HashMap;
import org.apache.camel.CamelContext;

/**
 * What an Event Hubs consumer needs from the node (design
 * §5.3.16): its checkpoint store, in a blob container, and its start
 * positions. Both are bound in Camel's registry under the names its URI
 * refers to.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class EventHubsSupport
{

    /** Not instantiated. */
    private EventHubsSupport()
    {
    }

    /**
     * Binds the checkpoint store and start positions in a Camel context.
     *
     * @param camel    the context
     * @param settings where the checkpoints are kept
     */
    public static void bind(CamelContext camel,
            ReceiveProperties.Eventhubs settings)
    {
        camel.getRegistry().bind(ReceiveSettings.CHECKPOINTS_BEAN,
                checkpoints(settings));
        camel.getRegistry().bind(ReceiveSettings.POSITIONS_BEAN,
                new EarliestPositions());
    }

    /**
     * Makes the checkpoint store, in its blob container, which must exist.
     *
     * @param settings where the checkpoints are kept
     * @return the store
     */
    static CheckpointStore checkpoints(ReceiveProperties.Eventhubs settings)
    {
        return new BlobCheckpointStore(container(settings));
    }

    /**
     * The checkpoint container's client: by its connection string, or by
     * its endpoint with the default Azure credential.
     *
     * @param settings where the checkpoints are kept
     * @return the client
     */
    static BlobContainerAsyncClient container(
            ReceiveProperties.Eventhubs settings)
    {
        BlobContainerClientBuilder container = new BlobContainerClientBuilder()
                .containerName(settings.checkpointContainer());
        String connection = settings.checkpointConnectionString();
        if (connection != null && !connection.isBlank())
        {
            container.connectionString(connection);
        }
        else
        {
            container.endpoint(settings.checkpointEndpoint())
                    .credential(new DefaultAzureCredentialBuilder().build());
        }
        return container.buildAsyncClient();
    }

    /**
     * Start positions: the earliest event, for every partition. The
     * processor uses one only for a partition with no checkpoint, so a new
     * consumer takes what the hub still holds, as Kafka's
     * {@code autoOffsetReset=earliest} does. The client asks by partition
     * ID, through {@link #get}.
     */
    static final class EarliestPositions extends HashMap<String, EventPosition>
    {

        /** For serialization. */
        private static final long serialVersionUID = 1L;

        @Override
        public EventPosition get(Object partitionId)
        {
            return EventPosition.earliest();
        }

        @Override
        public EventPosition getOrDefault(Object partitionId,
                EventPosition fallback)
        {
            return EventPosition.earliest();
        }
    }
}
