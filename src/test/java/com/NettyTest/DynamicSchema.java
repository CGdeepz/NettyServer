package com.NettyTest;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;

public class DynamicSchema {
	public static Builder newBuilder() {
		return new Builder();
	}

	public static DynamicSchema parseFrom(InputStream schemaDescIn) throws DescriptorValidationException, IOException {
		try {
			int len;
			byte[] buf = new byte[4096];
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			while ((len = schemaDescIn.read(buf)) > 0)
				baos.write(buf, 0, len);
			return parseFrom(baos.toByteArray());
		} finally {
			schemaDescIn.close();
		}
	}

	public static DynamicSchema parseFrom(byte[] schemaDescBuf) throws DescriptorValidationException, IOException {
		return new DynamicSchema(FileDescriptorSet.parseFrom(schemaDescBuf));
	}

	public DynamicMessage.Builder newMessageBuilder(String msgTypeName) {
		Descriptor msgType = getMessageDescriptor(msgTypeName);
		if (msgType == null)
			return null;
		return DynamicMessage.newBuilder(msgType);
	}

	public Descriptor getMessageDescriptor(String msgTypeName) {
		Descriptor msgType = mMsgDescriptorMapShort.get(msgTypeName);
		if (msgType == null)
			msgType = mMsgDescriptorMapFull.get(msgTypeName);
		return msgType;
	}

	public EnumValueDescriptor getEnumValue(String enumTypeName, String enumName) {
		EnumDescriptor enumType = getEnumDescriptor(enumTypeName);
		if (enumType == null)
			return null;
		return enumType.findValueByName(enumName);
	}

	public EnumValueDescriptor getEnumValue(String enumTypeName, int enumNumber) {
		EnumDescriptor enumType = getEnumDescriptor(enumTypeName);
		if (enumType == null)
			return null;
		return enumType.findValueByNumber(enumNumber);
	}

	public EnumDescriptor getEnumDescriptor(String enumTypeName) {
		EnumDescriptor enumType = mEnumDescriptorMapShort.get(enumTypeName);
		if (enumType == null)
			enumType = mEnumDescriptorMapFull.get(enumTypeName);
		return enumType;
	}

	public Set<String> getMessageTypes() {
		return new TreeSet<String>(mMsgDescriptorMapFull.keySet());
	}

	public Set<String> getEnumTypes() {
		return new TreeSet<String>(mEnumDescriptorMapFull.keySet());
	}

	public byte[] toByteArray() {
		return mFileDescSet.toByteArray();
	}

	public String toString() {
		Set<String> msgTypes = getMessageTypes();
		Set<String> enumTypes = getEnumTypes();
		return "types: " + msgTypes + "\nenums: " + enumTypes + "\n" + mFileDescSet;
	}

	private DynamicSchema(FileDescriptorSet fileDescSet) throws DescriptorValidationException {
		mFileDescSet = fileDescSet;
		Map<String, FileDescriptor> fileDescMap = init(fileDescSet);

		Set<String> msgDupes = new HashSet<String>();
		Set<String> enumDupes = new HashSet<String>();
		for (FileDescriptor fileDesc : fileDescMap.values()) {
			for (Descriptor msgType : fileDesc.getMessageTypes())
				addMessageType(msgType, null, msgDupes, enumDupes);
			for (EnumDescriptor enumType : fileDesc.getEnumTypes())
				addEnumType(enumType, null, enumDupes);
		}

		for (String msgName : msgDupes)
			mMsgDescriptorMapShort.remove(msgName);
		for (String enumName : enumDupes)
			mEnumDescriptorMapShort.remove(enumName);
	}

	@SuppressWarnings("unchecked")
	private Map<String, FileDescriptor> init(FileDescriptorSet fileDescSet) throws DescriptorValidationException {
		Map<String, FileDescriptor> fileDescMap = new HashMap<String, FileDescriptor>();
		for (FileDescriptorProto fdProto : fileDescSet.getFileList()) {
			if (fileDescMap.containsKey(fdProto.getName()))
				throw new IllegalArgumentException("duplicate name: " + fdProto.getName());
			fileDescMap.put(fdProto.getName(), null);
		}
		fileDescMap.clear();

		while (fileDescMap.size() < fileDescSet.getFileCount()) {
			for (FileDescriptorProto fdProto : fileDescSet.getFileList()) {
				if (fileDescMap.containsKey(fdProto.getName()))
					continue;
				List<String> dependencyList = null;
				try {
					Method m = fdProto.getClass().getMethod("getDependencyList", (Class<?>[]) null);
					dependencyList = (List<String>) m.invoke(fdProto, (Object[]) null);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}

				List<FileDescriptor> fdList = new ArrayList<FileDescriptor>();
				for (String depName : dependencyList) {
					FileDescriptor fd = fileDescMap.get(depName);
					if (fd != null)
						fdList.add(fd);
				}

				if (fdList.size() == dependencyList.size()) { // dependencies
																// resolved
					FileDescriptor[] fds = new FileDescriptor[fdList.size()];
					FileDescriptor fd = FileDescriptor.buildFrom(fdProto, fdList.toArray(fds));
					fileDescMap.put(fdProto.getName(), fd);
				}
			}
		}

		return fileDescMap;
	}

	private void addMessageType(Descriptor msgType, String scope, Set<String> msgDupes, Set<String> enumDupes) {
		String msgTypeNameFull = msgType.getFullName();
		String msgTypeNameShort = (scope == null ? msgType.getName() : scope + "." + msgType.getName());

		if (mMsgDescriptorMapFull.containsKey(msgTypeNameFull))
			throw new IllegalArgumentException("duplicate name: " + msgTypeNameFull);
		if (mMsgDescriptorMapShort.containsKey(msgTypeNameShort))
			msgDupes.add(msgTypeNameShort);

		mMsgDescriptorMapFull.put(msgTypeNameFull, msgType);
		mMsgDescriptorMapShort.put(msgTypeNameShort, msgType);

		for (Descriptor nestedType : msgType.getNestedTypes())
			addMessageType(nestedType, msgTypeNameShort, msgDupes, enumDupes);
		for (EnumDescriptor enumType : msgType.getEnumTypes())
			addEnumType(enumType, msgTypeNameShort, enumDupes);
	}

	private void addEnumType(EnumDescriptor enumType, String scope, Set<String> enumDupes) {
		String enumTypeNameFull = enumType.getFullName();
		String enumTypeNameShort = (scope == null ? enumType.getName() : scope + "." + enumType.getName());

		if (mEnumDescriptorMapFull.containsKey(enumTypeNameFull))
			throw new IllegalArgumentException("duplicate name: " + enumTypeNameFull);
		if (mEnumDescriptorMapShort.containsKey(enumTypeNameShort))
			enumDupes.add(enumTypeNameShort);

		mEnumDescriptorMapFull.put(enumTypeNameFull, enumType);
		mEnumDescriptorMapShort.put(enumTypeNameShort, enumType);
	}

	private FileDescriptorSet mFileDescSet;
	private Map<String, Descriptor> mMsgDescriptorMapFull = new HashMap<String, Descriptor>();
	private Map<String, Descriptor> mMsgDescriptorMapShort = new HashMap<String, Descriptor>();
	private Map<String, EnumDescriptor> mEnumDescriptorMapFull = new HashMap<String, EnumDescriptor>();
	private Map<String, EnumDescriptor> mEnumDescriptorMapShort = new HashMap<String, EnumDescriptor>();

	public static class Builder {
		public DynamicSchema build() throws DescriptorValidationException {
			FileDescriptorSet.Builder fileDescSetBuilder = FileDescriptorSet.newBuilder();
			fileDescSetBuilder.addFile(mFileDescProtoBuilder.build());
			fileDescSetBuilder.mergeFrom(mFileDescSetBuilder.build());
			return new DynamicSchema(fileDescSetBuilder.build());
		}

		public Builder setName(String name) {
			mFileDescProtoBuilder.setName(name);
			return this;
		}

		public Builder setPackage(String name) {
			mFileDescProtoBuilder.setPackage(name);
			return this;
		}

		public Builder addMessageDefinition(MessageDefinition msgDef) {
			mFileDescProtoBuilder.addMessageType(msgDef.getMessageType());
			return this;
		}

		public Builder addEnumDefinition(EnumDefinition enumDef) {
			mFileDescProtoBuilder.addEnumType(enumDef.getEnumType());
			return this;
		}

		public Builder addSchema(DynamicSchema schema) {
			mFileDescSetBuilder.mergeFrom(schema.mFileDescSet);
			return this;
		}

		// --- private ---

		private Builder() {
			mFileDescProtoBuilder = FileDescriptorProto.newBuilder();
			mFileDescSetBuilder = FileDescriptorSet.newBuilder();
		}

		private FileDescriptorProto.Builder mFileDescProtoBuilder;
		private FileDescriptorSet.Builder mFileDescSetBuilder;
	}
}