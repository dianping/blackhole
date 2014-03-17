package com.dp.blackhole.supervisor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ConsumerGroupDesc {
	private ConsumerGroup group;
	private Map<String, PartitionInfo> partitions;
	private Map<Consumer, ArrayList<String>> consumeMap;
	
	public static final Log LOG = LogFactory.getLog(Supervisor.class);
	
	public ConsumerGroupDesc(ConsumerGroup group, Collection<PartitionInfo> pinfos) {
		this.group = group;
		partitions = Collections.synchronizedMap(new HashMap<String, PartitionInfo>());
		for (PartitionInfo pinfo: pinfos) {
			partitions.put(pinfo.getId(),new PartitionInfo(pinfo));
		}
		consumeMap = Collections.synchronizedMap(new HashMap<Consumer, ArrayList<String>>());
	}

	public boolean exists (Consumer consumer) {
		return consumeMap.containsKey(consumer);
	}

	public Map<String, PartitionInfo> getPartitions () {
		return partitions;
	}
	
	public Map<Consumer, ArrayList<String>> getConsumeMap () {
		return consumeMap;
	}

	public void unregisterConsumer(Consumer consumer) {
		consumeMap.remove(consumer);
	}

	public Collection<Consumer> getConsumers() {
		return consumeMap.keySet();
	}

	public void updateOffset(String consumerId, String topic, String partition, long offset) {
		PartitionInfo info = partitions.get(partition);
		if (info == null) {
			LOG.error("can not find PartitionInfo by partition: " + "[" + topic +"]" + partition + " ,request from " + consumerId);
		}
		info.setEndOffset(offset);
	}
	
}
