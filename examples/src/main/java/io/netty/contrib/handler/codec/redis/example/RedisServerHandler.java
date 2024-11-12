package io.netty.contrib.handler.codec.redis.example;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.contrib.handler.codec.redis.*;
import io.netty.util.ReferenceCountUtil;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public final class RedisServerHandler extends ChannelInboundHandlerAdapter {
    private final ConcurrentHashMap<String,String> map;
    private final CountDownLatch countDownLatch;

    public RedisServerHandler(ConcurrentHashMap<String, String> map, CountDownLatch countDownLatch) {
        this.map = map;
        this.countDownLatch = countDownLatch;
    }


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        try{
            if (!(msg instanceof ArrayRedisMessage) ) {
                reject(ctx, "에러이레디스메시지 아니래");
                return;
            }

            final ArrayRedisMessage req = (ArrayRedisMessage) msg;
            final List<RedisMessage> args = req.children();

            for (RedisMessage a : args) {
                if (!(a instanceof FullBulkStringRedisMessage)) {
                    reject(ctx, "풀벌크스트링이 아닌가봐");
                    return ;
                }
            }

            final List<String> strArgs = args.stream().map(a -> {
                final FullBulkStringRedisMessage fullBulkStringRedisMessage = (FullBulkStringRedisMessage) a;

                if (!(fullBulkStringRedisMessage.isNull())) {
                    return fullBulkStringRedisMessage.content().toString(StandardCharsets.UTF_8);
                } else return null;

            }).collect(Collectors.toList());


            final String command = strArgs.get(0);

            switch (command) {
                case "COMMAND":
                    ctx.writeAndFlush(new SimpleStringRedisMessage("COMMAND INPUT"));
                    break;
                case "GET":
                    commandGET(ctx, strArgs);
                    break;
                case "SET":
                    commandSET(ctx, strArgs);
                    break;
                case "DEL":
                    commandDEL(ctx, strArgs);
                    break;

                case "SHUTDOWN":
                    ctx.writeAndFlush(new SimpleStringRedisMessage("OK"))
                            .addListener((ChannelFutureListener) f -> {
                                countDownLatch.countDown();
                            });
                default:
                    ctx.writeAndFlush(new SimpleStringRedisMessage("메롱"));
                    break;

            }




        }
        finally {
            ReferenceCountUtil.release(msg);
        }






    }

    private void commandDEL(ChannelHandlerContext ctx, List<String> strArgs) {
        if(strArgs.size() <2){
            reject(ctx, "commandDEL SIZE ERROR");
            return;
        }

        final String key = strArgs.get(1);

        if(key == null) {
            reject(ctx, "commandDEL KEY ERROR");
            return;
        }

        final String value = map.remove(key);
        final FullBulkStringRedisMessage msg;

        if (value == null)  msg = FullBulkStringRedisMessage.NULL_INSTANCE;
        else msg = newFullBulkStringRedisMessage(value);


        ctx.writeAndFlush(msg);


    }

    private void commandSET(ChannelHandlerContext ctx, List<String> strArgs) {
        if(strArgs.size() < 3){
            reject(ctx, "commandSET SIZE ERROR");
            return;
        }

        final String key = strArgs.get(1);
        final String value = strArgs.get(2);

        if(key == null || value == null) {
            reject(ctx, "commandSET NULL ERROR");
            return;
        }

        final boolean shouldReplyOldValue =
                strArgs.size() > 3 && "GET".equals(strArgs.get(3));
        final String oldValue = map.put(key, value);
        final RedisMessage msg;

        if (shouldReplyOldValue) {
            if (oldValue != null) {
                msg = newFullBulkStringRedisMessage(oldValue);
            } else {
                msg = FullBulkStringRedisMessage.NULL_INSTANCE;
            }
        } else {
            msg = new SimpleStringRedisMessage("OK");
        }

        ctx.writeAndFlush(msg);




    }

    private void commandGET(ChannelHandlerContext ctx, List<String> strArgs) {
        if(strArgs.size() <2){
            reject(ctx, "commandGET SIZE ERROR");
            return;
        }

        final String key = strArgs.get(1);

        if(key == null) {
            reject(ctx, "commandNULL SIZE ERROR");
            return;
        }

        final String value = map.get(key);
        final FullBulkStringRedisMessage msg;
        if (value == null)  msg = FullBulkStringRedisMessage.NULL_INSTANCE;
        else msg = newFullBulkStringRedisMessage(value);


        ctx.writeAndFlush(msg);


    }

    private static void reject(ChannelHandlerContext ctx, String error) {
        ctx.writeAndFlush(new ErrorRedisMessage(error));
    }


    private static FullBulkStringRedisMessage newFullBulkStringRedisMessage(String value) {
        return new FullBulkStringRedisMessage(Unpooled.copiedBuffer(value, StandardCharsets.UTF_8));
    }

}
