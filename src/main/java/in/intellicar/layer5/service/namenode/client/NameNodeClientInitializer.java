package in.intellicar.layer5.service.namenode.client;

import in.intellicar.layer5.beacon.Layer5BeaconParser;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaBeacon;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.vertx.core.Vertx;

import java.util.logging.Logger;

public class NameNodeClientInitializer extends ChannelInitializer<SocketChannel>  {
    private Vertx vertx;
    private Logger logger;

    public NameNodeClientInitializer(Vertx Vertx, Logger lLogger) {
        this.logger = lLogger;
        this.vertx = vertx;
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        ChannelPipeline pipeline = socketChannel.pipeline();
        pipeline.addLast("client handler", new NameNodeClientHandler(Layer5BeaconParser.getHandler("ClientInit", logger),
                vertx, logger));
    }
}
