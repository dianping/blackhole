// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: ConsumerExit.proto

package com.dp.blackhole.protocol.control;

public final class ConsumerExitPB {
  private ConsumerExitPB() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface ConsumerExitOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // required string groupId = 1;
    /**
     * <code>required string groupId = 1;</code>
     */
    boolean hasGroupId();
    /**
     * <code>required string groupId = 1;</code>
     */
    java.lang.String getGroupId();
    /**
     * <code>required string groupId = 1;</code>
     */
    com.google.protobuf.ByteString
        getGroupIdBytes();

    // required string consumerId = 2;
    /**
     * <code>required string consumerId = 2;</code>
     */
    boolean hasConsumerId();
    /**
     * <code>required string consumerId = 2;</code>
     */
    java.lang.String getConsumerId();
    /**
     * <code>required string consumerId = 2;</code>
     */
    com.google.protobuf.ByteString
        getConsumerIdBytes();

    // required string topic = 3;
    /**
     * <code>required string topic = 3;</code>
     */
    boolean hasTopic();
    /**
     * <code>required string topic = 3;</code>
     */
    java.lang.String getTopic();
    /**
     * <code>required string topic = 3;</code>
     */
    com.google.protobuf.ByteString
        getTopicBytes();
  }
  /**
   * Protobuf type {@code blackhole.ConsumerExit}
   */
  public static final class ConsumerExit extends
      com.google.protobuf.GeneratedMessage
      implements ConsumerExitOrBuilder {
    // Use ConsumerExit.newBuilder() to construct.
    private ConsumerExit(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private ConsumerExit(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final ConsumerExit defaultInstance;
    public static ConsumerExit getDefaultInstance() {
      return defaultInstance;
    }

    public ConsumerExit getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private ConsumerExit(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              groupId_ = input.readBytes();
              break;
            }
            case 18: {
              bitField0_ |= 0x00000002;
              consumerId_ = input.readBytes();
              break;
            }
            case 26: {
              bitField0_ |= 0x00000004;
              topic_ = input.readBytes();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.dp.blackhole.protocol.control.ConsumerExitPB.internal_static_blackhole_ConsumerExit_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.dp.blackhole.protocol.control.ConsumerExitPB.internal_static_blackhole_ConsumerExit_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit.class, com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit.Builder.class);
    }

    public static com.google.protobuf.Parser<ConsumerExit> PARSER =
        new com.google.protobuf.AbstractParser<ConsumerExit>() {
      public ConsumerExit parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new ConsumerExit(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<ConsumerExit> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // required string groupId = 1;
    public static final int GROUPID_FIELD_NUMBER = 1;
    private java.lang.Object groupId_;
    /**
     * <code>required string groupId = 1;</code>
     */
    public boolean hasGroupId() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required string groupId = 1;</code>
     */
    public java.lang.String getGroupId() {
      java.lang.Object ref = groupId_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          groupId_ = s;
        }
        return s;
      }
    }
    /**
     * <code>required string groupId = 1;</code>
     */
    public com.google.protobuf.ByteString
        getGroupIdBytes() {
      java.lang.Object ref = groupId_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        groupId_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    // required string consumerId = 2;
    public static final int CONSUMERID_FIELD_NUMBER = 2;
    private java.lang.Object consumerId_;
    /**
     * <code>required string consumerId = 2;</code>
     */
    public boolean hasConsumerId() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>required string consumerId = 2;</code>
     */
    public java.lang.String getConsumerId() {
      java.lang.Object ref = consumerId_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          consumerId_ = s;
        }
        return s;
      }
    }
    /**
     * <code>required string consumerId = 2;</code>
     */
    public com.google.protobuf.ByteString
        getConsumerIdBytes() {
      java.lang.Object ref = consumerId_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        consumerId_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    // required string topic = 3;
    public static final int TOPIC_FIELD_NUMBER = 3;
    private java.lang.Object topic_;
    /**
     * <code>required string topic = 3;</code>
     */
    public boolean hasTopic() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>required string topic = 3;</code>
     */
    public java.lang.String getTopic() {
      java.lang.Object ref = topic_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          topic_ = s;
        }
        return s;
      }
    }
    /**
     * <code>required string topic = 3;</code>
     */
    public com.google.protobuf.ByteString
        getTopicBytes() {
      java.lang.Object ref = topic_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        topic_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private void initFields() {
      groupId_ = "";
      consumerId_ = "";
      topic_ = "";
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasGroupId()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasConsumerId()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasTopic()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, getGroupIdBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeBytes(2, getConsumerIdBytes());
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeBytes(3, getTopicBytes());
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, getGroupIdBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(2, getConsumerIdBytes());
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(3, getTopicBytes());
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code blackhole.ConsumerExit}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExitOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.dp.blackhole.protocol.control.ConsumerExitPB.internal_static_blackhole_ConsumerExit_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.dp.blackhole.protocol.control.ConsumerExitPB.internal_static_blackhole_ConsumerExit_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit.class, com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit.Builder.class);
      }

      // Construct using com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        groupId_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        consumerId_ = "";
        bitField0_ = (bitField0_ & ~0x00000002);
        topic_ = "";
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.dp.blackhole.protocol.control.ConsumerExitPB.internal_static_blackhole_ConsumerExit_descriptor;
      }

      public com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit getDefaultInstanceForType() {
        return com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit.getDefaultInstance();
      }

      public com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit build() {
        com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit buildPartial() {
        com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit result = new com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.groupId_ = groupId_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.consumerId_ = consumerId_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.topic_ = topic_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit) {
          return mergeFrom((com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit other) {
        if (other == com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit.getDefaultInstance()) return this;
        if (other.hasGroupId()) {
          bitField0_ |= 0x00000001;
          groupId_ = other.groupId_;
          onChanged();
        }
        if (other.hasConsumerId()) {
          bitField0_ |= 0x00000002;
          consumerId_ = other.consumerId_;
          onChanged();
        }
        if (other.hasTopic()) {
          bitField0_ |= 0x00000004;
          topic_ = other.topic_;
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasGroupId()) {
          
          return false;
        }
        if (!hasConsumerId()) {
          
          return false;
        }
        if (!hasTopic()) {
          
          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // required string groupId = 1;
      private java.lang.Object groupId_ = "";
      /**
       * <code>required string groupId = 1;</code>
       */
      public boolean hasGroupId() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required string groupId = 1;</code>
       */
      public java.lang.String getGroupId() {
        java.lang.Object ref = groupId_;
        if (!(ref instanceof java.lang.String)) {
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          groupId_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>required string groupId = 1;</code>
       */
      public com.google.protobuf.ByteString
          getGroupIdBytes() {
        java.lang.Object ref = groupId_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          groupId_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>required string groupId = 1;</code>
       */
      public Builder setGroupId(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        groupId_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required string groupId = 1;</code>
       */
      public Builder clearGroupId() {
        bitField0_ = (bitField0_ & ~0x00000001);
        groupId_ = getDefaultInstance().getGroupId();
        onChanged();
        return this;
      }
      /**
       * <code>required string groupId = 1;</code>
       */
      public Builder setGroupIdBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        groupId_ = value;
        onChanged();
        return this;
      }

      // required string consumerId = 2;
      private java.lang.Object consumerId_ = "";
      /**
       * <code>required string consumerId = 2;</code>
       */
      public boolean hasConsumerId() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>required string consumerId = 2;</code>
       */
      public java.lang.String getConsumerId() {
        java.lang.Object ref = consumerId_;
        if (!(ref instanceof java.lang.String)) {
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          consumerId_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>required string consumerId = 2;</code>
       */
      public com.google.protobuf.ByteString
          getConsumerIdBytes() {
        java.lang.Object ref = consumerId_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          consumerId_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>required string consumerId = 2;</code>
       */
      public Builder setConsumerId(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        consumerId_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required string consumerId = 2;</code>
       */
      public Builder clearConsumerId() {
        bitField0_ = (bitField0_ & ~0x00000002);
        consumerId_ = getDefaultInstance().getConsumerId();
        onChanged();
        return this;
      }
      /**
       * <code>required string consumerId = 2;</code>
       */
      public Builder setConsumerIdBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        consumerId_ = value;
        onChanged();
        return this;
      }

      // required string topic = 3;
      private java.lang.Object topic_ = "";
      /**
       * <code>required string topic = 3;</code>
       */
      public boolean hasTopic() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>required string topic = 3;</code>
       */
      public java.lang.String getTopic() {
        java.lang.Object ref = topic_;
        if (!(ref instanceof java.lang.String)) {
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          topic_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>required string topic = 3;</code>
       */
      public com.google.protobuf.ByteString
          getTopicBytes() {
        java.lang.Object ref = topic_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          topic_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>required string topic = 3;</code>
       */
      public Builder setTopic(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        topic_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required string topic = 3;</code>
       */
      public Builder clearTopic() {
        bitField0_ = (bitField0_ & ~0x00000004);
        topic_ = getDefaultInstance().getTopic();
        onChanged();
        return this;
      }
      /**
       * <code>required string topic = 3;</code>
       */
      public Builder setTopicBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        topic_ = value;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:blackhole.ConsumerExit)
    }

    static {
      defaultInstance = new ConsumerExit(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:blackhole.ConsumerExit)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_blackhole_ConsumerExit_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_blackhole_ConsumerExit_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\022ConsumerExit.proto\022\tblackhole\"B\n\014Consu" +
      "merExit\022\017\n\007groupId\030\001 \002(\t\022\022\n\nconsumerId\030\002" +
      " \002(\t\022\r\n\005topic\030\003 \002(\tB3\n!com.dp.blackhole." +
      "protocol.controlB\016ConsumerExitPB"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_blackhole_ConsumerExit_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_blackhole_ConsumerExit_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_blackhole_ConsumerExit_descriptor,
              new java.lang.String[] { "GroupId", "ConsumerId", "Topic", });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}
