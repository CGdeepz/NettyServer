package com.NettyTest.Service;

import com.NettyTest.ConvertToProtoBuff.GetQueryParamAndConvertToProtoBuff;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

public class WelcomeService extends ChannelInitializer<SocketChannel> {

	protected void initChannel(SocketChannel socketChannel) throws Exception {
		try {
			ChannelPipeline p = socketChannel.pipeline();
			p.addLast("decoder", new HttpRequestDecoder());
			p.addLast("encoder", new HttpResponseEncoder());
			p.addLast("httpHandler", new GetQueryParamAndConvertToProtoBuff());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
