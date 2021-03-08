package in.intellicar.layer5.service.namenode.mysql;

import in.intellicar.layer5.beacon.storagemetacls.PayloadTypes;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaPayload;
import in.intellicar.layer5.beacon.storagemetacls.payload.StorageClsMetaErrorRsp;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.client.AccIdGenerateReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.client.AccIdGenerateRsp;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.internal.AccIdRegisterReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.internal.AccIdRegisterRsp;
import in.intellicar.layer5.beacon.storagemetacls.service.common.IPayloadRequestHandler;
import in.intellicar.layer5.service.namenode.utils.NameNodeUtils;
import in.intellicar.layer5.utils.sha.SHA256Item;
import io.vertx.core.Future;
import io.vertx.mysqlclient.MySQLPool;

import java.util.logging.Logger;

/**
 * @author krishna mohan
 * @version 1.0
 * @project StorageClsAccIDMetaService
 * @date 02/03/21 - 5:09 PM
 */
public class NameNodePayloadHandler implements IPayloadRequestHandler {
    @Override
    public StorageClsMetaPayload getResponsePayload(StorageClsMetaPayload lRequestPayload, MySQLPool lVertxMySQLClient, Logger lLogger) {
        short subType = lRequestPayload.getSubType();
        PayloadTypes payloadType = PayloadTypes.getPayloadType(subType);

        switch (payloadType) {
            case ACCOUNT_ID_GEN_REQ:
                Future<SHA256Item> accountIDFuture = NameNodeUtils.generateAccountID((AccIdGenerateReq) lRequestPayload, lVertxMySQLClient, lLogger);
                if (accountIDFuture.succeeded()) {
                    return null;
//                    return accountIDFuture.result();
                } else {
                    return new StorageClsMetaErrorRsp(accountIDFuture.cause().getLocalizedMessage(), lRequestPayload);
                }
            case ACCOUNT_ID_REG_REQ:
                Future<AccIdRegisterRsp> ack = NameNodeUtils.registerAccountID((AccIdRegisterReq) lRequestPayload, lVertxMySQLClient, lLogger);
                if (ack.succeeded()) {
                    return ack.result();
                } else {
                    return new StorageClsMetaErrorRsp(ack.cause().getLocalizedMessage(), lRequestPayload);
                }
            default:
                return new StorageClsMetaErrorRsp("Sent Unknown PayloadType",  lRequestPayload);
        }
    }
}
