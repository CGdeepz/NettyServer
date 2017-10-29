package com.NettyTest;

import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;

public class EnumDefinition {

	public static Builder newBuilder(String enumName) {
		return new Builder(enumName);
	}

	public String toString() {
		return mEnumType.toString();
	}

	EnumDescriptorProto getEnumType() {
		return mEnumType;
	}

	private EnumDefinition(EnumDescriptorProto enumType) {
		mEnumType = enumType;
	}

	private EnumDescriptorProto mEnumType;

	public static class Builder {

		public Builder addValue(String name, int num) {
			EnumValueDescriptorProto.Builder enumValBuilder = EnumValueDescriptorProto.newBuilder();
			enumValBuilder.setName(name).setNumber(num);
			mEnumTypeBuilder.addValue(enumValBuilder.build());
			return this;
		}

		public EnumDefinition build() {
			return new EnumDefinition(mEnumTypeBuilder.build());
		}

		private Builder(String enumName) {
			mEnumTypeBuilder = EnumDescriptorProto.newBuilder();
			mEnumTypeBuilder.setName(enumName);
		}

		private EnumDescriptorProto.Builder mEnumTypeBuilder;
	}
}