package com.NettyTest;

import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;

public class MessageDefinition {

	public static Builder newBuilder(String msgTypeName) {
		return new Builder(msgTypeName);
	}

	public String toString() {
		return mMsgType.toString();
	}

	DescriptorProto getMessageType() {
		return mMsgType;
	}

	private MessageDefinition(DescriptorProto msgType) {
		mMsgType = msgType;
	}

	private DescriptorProto mMsgType;

	public static class Builder {

		public Builder addField(String label, String type, String name, int num) {
			return addField(label, type, name, num, null);
		}

		public Builder addField(String label, String type, String name, int num, String defaultVal) {
			FieldDescriptorProto.Label protoLabel = sLabelMap.get(label);
			if (protoLabel == null)
				throw new IllegalArgumentException("Illegal label: " + label);
			addField(protoLabel, type, name, num, defaultVal);
			return this;
		}

		public Builder addMessageDefinition(MessageDefinition msgDef) {
			mMsgTypeBuilder.addNestedType(msgDef.getMessageType());
			return this;
		}

		public Builder addEnumDefinition(EnumDefinition enumDef) {
			mMsgTypeBuilder.addEnumType(enumDef.getEnumType());
			return this;
		}

		public MessageDefinition build() {
			return new MessageDefinition(mMsgTypeBuilder.build());
		}

		// --- private ---

		private Builder(String msgTypeName) {
			mMsgTypeBuilder = DescriptorProto.newBuilder();
			mMsgTypeBuilder.setName(msgTypeName);
		}

		private void addField(FieldDescriptorProto.Label label, String type, String name, int num, String defaultVal) {
			FieldDescriptorProto.Builder fieldBuilder = FieldDescriptorProto.newBuilder();
			fieldBuilder.setLabel(label);
			FieldDescriptorProto.Type primType = sTypeMap.get(type);
			if (primType != null)
				fieldBuilder.setType(primType);
			else
				fieldBuilder.setTypeName(type);
			fieldBuilder.setName(name).setNumber(num);
			if (defaultVal != null)
				fieldBuilder.setDefaultValue(defaultVal);
			mMsgTypeBuilder.addField(fieldBuilder.build());
		}

		private DescriptorProto.Builder mMsgTypeBuilder;
	}

	private static Map<String, FieldDescriptorProto.Type> sTypeMap;
	private static Map<String, FieldDescriptorProto.Label> sLabelMap;

	static {
		sTypeMap = new HashMap<String, FieldDescriptorProto.Type>();
		sTypeMap.put("double", FieldDescriptorProto.Type.TYPE_DOUBLE);
		sTypeMap.put("float", FieldDescriptorProto.Type.TYPE_FLOAT);
		sTypeMap.put("int32", FieldDescriptorProto.Type.TYPE_INT32);
		sTypeMap.put("int64", FieldDescriptorProto.Type.TYPE_INT64);
		sTypeMap.put("uint32", FieldDescriptorProto.Type.TYPE_UINT32);
		sTypeMap.put("uint64", FieldDescriptorProto.Type.TYPE_UINT64);
		sTypeMap.put("sint32", FieldDescriptorProto.Type.TYPE_SINT32);
		sTypeMap.put("sint64", FieldDescriptorProto.Type.TYPE_SINT64);
		sTypeMap.put("fixed32", FieldDescriptorProto.Type.TYPE_FIXED32);
		sTypeMap.put("fixed64", FieldDescriptorProto.Type.TYPE_FIXED64);
		sTypeMap.put("sfixed32", FieldDescriptorProto.Type.TYPE_SFIXED32);
		sTypeMap.put("sfixed64", FieldDescriptorProto.Type.TYPE_SFIXED64);
		sTypeMap.put("bool", FieldDescriptorProto.Type.TYPE_BOOL);
		sTypeMap.put("string", FieldDescriptorProto.Type.TYPE_STRING);
		sTypeMap.put("bytes", FieldDescriptorProto.Type.TYPE_BYTES);

		sLabelMap = new HashMap<String, FieldDescriptorProto.Label>();
		sLabelMap.put("optional", FieldDescriptorProto.Label.LABEL_OPTIONAL);
		sLabelMap.put("required", FieldDescriptorProto.Label.LABEL_REQUIRED);
		sLabelMap.put("repeated", FieldDescriptorProto.Label.LABEL_REPEATED);
	}
}