package io.netty.contrib.handler.codec.redis.example;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.contrib.handler.codec.redis.RedisArrayAggregator;
import io.netty.contrib.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.contrib.handler.codec.redis.RedisDecoder;
import io.netty.contrib.handler.codec.redis.RedisEncoder;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class RedisServer {
    private static final int PORT = Integer.parseInt(System.getProperty("port", "6379"));


    public static void main(String[] args) throws Exception {
        final NioEventLoopGroup mainLoop = new NioEventLoopGroup(0);
        final NioEventLoopGroup workLoop = new NioEventLoopGroup(1);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();


        try {
            final ServerBootstrap serverBootstrap = new ServerBootstrap();

            //init server config
            serverBootstrap.channel(NioServerSocketChannel.class);
            serverBootstrap.group(mainLoop, workLoop);

            serverBootstrap.childHandler(new ChannelInitializer<>() {
                @Override
                protected void initChannel(Channel channel) throws Exception {
                    ChannelPipeline channelPipeline = channel.pipeline();

                    channelPipeline.addLast(new RedisDecoder());
                    channelPipeline.addLast(new RedisBulkStringAggregator());
                    channelPipeline.addLast(new RedisArrayAggregator());
                    channelPipeline.addLast(new RedisEncoder());
                    channelPipeline.addLast(new RedisServerHandler(map, countDownLatch));

                }
            });

            final Channel channel = serverBootstrap.bind(PORT).sync().channel();
            System.out.println("channel bind complete : " + channel.localAddress() + " listening..");

            countDownLatch.await();
            System.out.println("close Server ");

        } finally {
            mainLoop.shutdownGracefully();
            workLoop.shutdownGracefully();
        }


    }

    private RedisServer(){}

}
