package com.dp.blackhole.node;

import com.dp.blackhole.common.MessagePB.Message;

public class Node {
  /**
   * Send message to supervisor
   * @param message
   */
  public void sendMessage(Message message) {
    // TODO Auto-generated method stub
    System.out.println("send message " + message.getType().name());
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

  public void loop() {
    // TODO Auto-generated method stub
    
  }
}
