package com.dp.blackhole.consumer;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.collectornode.persistent.protocol.DataMessageTypeFactory;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.network.DelegationIOConnection;
import com.dp.blackhole.network.GenClient;
import com.dp.blackhole.network.TransferWrap;

import static java.lang.String.format;

public class FetcherRunnable extends Thread {

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final String broker;

    private final List<PartitionTopicInfo> partitionTopicInfos;

    private final Log logger = LogFactory.getLog(FetcherRunnable.class);
    
    private GenClient<TransferWrap, DelegationIOConnection, ConsumerProcessor> client;
    
    public FetcherRunnable(String broker,
                           List<PartitionTopicInfo> partitionTopicInfos) {
        this.broker = broker;
        this.partitionTopicInfos = partitionTopicInfos;
    }

    public void shutdown() throws InterruptedException {
        logger.debug("shutdown the fetcher " + getName());
        interrupt();
        shutdownLatch.await(5, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        StringBuilder buf = new StringBuilder("[");
        for (PartitionTopicInfo pti : partitionTopicInfos) {
            buf.append(format("%s-%s-%s,", pti.topic, pti.brokerString, pti.partition));
        }
        buf.append(']');
        logger.info(String.format("%s comsume at %s:%d with %s", getName(), Util.getHostFromBroker(broker), Util.getPortFromBroker(broker), buf.toString()));

        client = new GenClient(
                new ConsumerProcessor(partitionTopicInfos, Consumer.config.isBetterOrdered()),
                new DelegationIOConnection.DelegationIOConnectionFactory(),
                new DataMessageTypeFactory());
        Properties prop = new Properties();
        prop.setProperty("Server.host", Util.getHostFromBroker(broker));
        prop.setProperty("Server.port", Util.getPortFromBroker(broker));
        try {
            client.init(prop, getName());
        } catch (ClosedChannelException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        logger.debug("stopping fetcher " + getName() + " to broker " + broker);
        shutdownLatch.countDown();
    }
}
