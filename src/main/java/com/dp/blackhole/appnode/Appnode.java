package com.dp.blackhole.appnode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.node.Node;
import com.dp.common.Message;

public class Appnode extends Node {

	  public static final Log LOG = LogFactory.getLog(Appnode.class);
	
	private boolean process(Message msg) {
		
		switch (msg.getHead()) {

		default:
			LOG.warn("unknow Message");
			
		}
		
		
		return true;		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
	}

}
