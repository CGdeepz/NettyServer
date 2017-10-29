package com.NettyTest.ConvertToProtoBuff;

import java.util.List;
import java.util.Map;

import com.google.protobuf.Descriptors.Descriptor;
import com.NettyTest.DynamicSchema;
import com.NettyTest.MessageDefinition;
import com.NettyTest.kafkaQueuePush.PushMessage;
import com.google.protobuf.DynamicMessage;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;

public class GetQueryParamAndConvertToProtoBuff extends ChannelInboundHandlerAdapter {

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msgRec) throws Exception {

		HttpRequest httpRequest = (HttpRequest) msgRec;
		QueryStringDecoder queryDecoder = new QueryStringDecoder(httpRequest.getUri(), true);
		Map<String, List<String>> parameters = queryDecoder.parameters();
		String name = parameters.get("name").get(0);

		DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
		schemaBuilder.setName("PersonSchemaDynamic.proto");

		MessageDefinition msgDef = MessageDefinition.newBuilder("Person").addField("required", "string", "name", 2)
				.build();

		schemaBuilder.addMessageDefinition(msgDef);
		DynamicSchema schema = schemaBuilder.build();

		DynamicMessage.Builder msgBuilder = schema.newMessageBuilder("Person");
		Descriptor msgDesc = msgBuilder.getDescriptorForType();
		DynamicMessage message = msgBuilder.setField(msgDesc.findFieldByName("name"), name).build();
		PushMessage.pushMessage(message);
	}
}