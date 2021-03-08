package in.intellicar.layer5.service.namenode.client;

import in.intellicar.layer5.beacon.Layer5Beacon;
import in.intellicar.layer5.beacon.Layer5BeaconDeserializer;
import in.intellicar.layer5.beacon.Layer5BeaconParser;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaBeacon;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaBeaconDeser;
import in.intellicar.layer5.beacon.storagemetacls.payload.metaclsservice.InstanceIdToBuckReq;
import in.intellicar.layer5.data.Deserialized;
import in.intellicar.layer5.utils.LittleEndianUtils;
import in.intellicar.layer5.utils.sha.SHA256Item;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.ArrayList;
import java.util.logging.Logger;

public class NameNodeClientHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private Logger logger;
    private Layer5BeaconParser l5parser;
    private StorageClsMetaBeacon beacon;
    private byte[] handlerBuffer;
    private int bufridx;
    private int bufwidx;
    private String serverName;
    private static int nameIncrementer = 0;

    public NameNodeClientHandler(Layer5BeaconParser l5parser, String serverName, StorageClsMetaBeacon beacon, Logger logger){
        this.l5parser = l5parser;
        this.serverName = serverName;
        this.beacon = beacon;
        this.logger = logger;

        Layer5BeaconDeserializer storageMetaClsAPIDeser = new StorageClsMetaBeaconDeser();
        l5parser.registerDeserializer(storageMetaClsAPIDeser.getBeaconType(), storageMetaClsAPIDeser);

        handlerBuffer = new byte[16 * 1024];
        bufridx = 0;
        bufwidx = 0;
    }

    public void channelActive(ChannelHandlerContext ctx) throws Exception{
        ctx.writeAndFlush(Unpooled.wrappedBuffer(returnSerializedByteStreamOfBeacon(beacon)));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        ByteBuf byteBuf = (ByteBuf) msg;
        logger.info("Received a message");

        ArrayList<StorageClsMetaBeacon> beaconsFound = new ArrayList<>();

        while (byteBuf.isReadable() && bufwidx < (handlerBuffer.length - 1024)) {
            int bytesToCopy = 1024;
            if (byteBuf.readableBytes() < 1024) {
                bytesToCopy = byteBuf.readableBytes();
            }
            byteBuf.readBytes(handlerBuffer, bufwidx, bytesToCopy);
            bufwidx += bytesToCopy;
            beaconsFound.addAll(parseLayer5Beacons());
        }

        //TODO: Send messages via eventbus
        logger.info("Total beacons Found: " + beaconsFound.size());
    }

    private byte[] returnSerializedByteStreamOfBeacon (StorageClsMetaBeacon lBeacon) {
        int beaconSize = lBeacon.getBeaconSize();
        byte[] beaconSerializedBuffer = new byte[beaconSize];
        l5parser.serialize(beaconSerializedBuffer, 0, beaconSize, lBeacon, logger);
        return beaconSerializedBuffer;
    }

    private ArrayList<StorageClsMetaBeacon> parseLayer5Beacons() {
        ArrayList<StorageClsMetaBeacon> beaconsFound = new ArrayList();
        while (l5parser.isHeaderAvailable(handlerBuffer, bufridx, bufwidx, logger)) {
            if (!l5parser.isHeaderValid(handlerBuffer, bufridx, bufwidx, logger)) {
                skipBytesTillHeader();
                continue;
            }
            if (!l5parser.isDataSufficient(handlerBuffer, bufridx, bufwidx, logger)) {
                break;
            }
            Deserialized<Layer5Beacon> layer5BeaconD = l5parser.deserialize(handlerBuffer, bufridx, bufwidx, logger);
            if (layer5BeaconD.data != null)
            {
                Layer5Beacon beacon = layer5BeaconD.data;
//                Append only StorageMetaClsBeacons
                if (beacon.getBeaconType() == 1 ) {
                    beaconsFound.add((StorageClsMetaBeacon) beacon);
                }
//                Ignore other beacons
                bufridx = layer5BeaconD.curridx;
                adjustBuffer();
            }
            else // Either deserializer not found or crc check failed. Here we are just breaking the loop.
            // Better to skip the beacon in _l5parser.deserializer
            {
                break;
            }
        }
        return beaconsFound;
    }

    private void adjustBuffer() {
        logger.info("Adjusting buffer: " + bufridx + " , " + bufwidx);
        if (bufwidx - bufridx > 0 && bufridx > 0) {
            System.arraycopy(handlerBuffer, bufridx, handlerBuffer, 0, bufwidx - bufridx);
            bufwidx -= bufridx;
            bufridx = 0;
        }
    }

    private void skipBytesTillHeader() {
        for (int i = bufridx; i < bufwidx; i++) {
//            if (!l5parser.isHeaderAvailable(handlerBuffer, bufridx, bufwidx, logger)){
//                break;
//            }
            if (l5parser.isHeaderValid(handlerBuffer, bufridx, bufwidx, logger)) {
                break;
            }
            bufridx++;
        }
        adjustBuffer();
    }

}