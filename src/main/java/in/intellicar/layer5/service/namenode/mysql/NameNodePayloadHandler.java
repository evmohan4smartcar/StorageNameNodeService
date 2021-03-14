package in.intellicar.layer5.service.namenode.mysql;

import in.intellicar.layer5.beacon.storagemetacls.PayloadTypes;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaPayload;
import in.intellicar.layer5.beacon.storagemetacls.payload.StorageClsMetaErrorRsp;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.client.*;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.internal.AccIdRegisterReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.internal.AccIdRegisterRsp;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.internal.NsIdRegisterReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.internal.NsIdRegisterRsp;
import in.intellicar.layer5.beacon.storagemetacls.service.common.IPayloadRequestHandler;
import in.intellicar.layer5.service.namenode.utils.NameNodeUtils;
import in.intellicar.layer5.utils.sha.SHA256Item;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
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
                return NameNodeUtils.getAccountID((AccIdGenerateReq) lRequestPayload, lVertxMySQLClient, lLogger);

            case ACCOUNT_ID_REG_REQ:
                return NameNodeUtils.registerAccountID((AccIdRegisterReq) lRequestPayload, lVertxMySQLClient, lLogger);

            case ACCOUNT_ID_REG_RSP://TODO:: using it to update ack for now, might need to change it in future
                NameNodeUtils.updateAckOfAccName((AccIdRegisterRsp) lRequestPayload, lVertxMySQLClient, lLogger);
                return lRequestPayload;

            case NS_ID_GEN_REQ:
                return NameNodeUtils.getNamespaceId((NsIdGenerateReq) lRequestPayload, lVertxMySQLClient, lLogger);

            case NS_ID_REG_REQ:
                return NameNodeUtils.registerNsId((NsIdRegisterReq) lRequestPayload, lVertxMySQLClient, lLogger);

            case NS_ID_REG_RSP://TODO:: using it to update ack for now, might need to change it in future
                NameNodeUtils.updateAckOfNsName((NsIdRegisterRsp) lRequestPayload, lVertxMySQLClient, lLogger);
                return lRequestPayload;

            case DIR_ID_GEN_REG_REQ:
                return NameNodeUtils.getDirId((DirIdGenerateAndRegisterReq) lRequestPayload, lVertxMySQLClient, lLogger);

            case FILE_ID_GEN_REG_REQ:
                return NameNodeUtils.getFileId((FileIdGenerateAndRegisterReq) lRequestPayload, lVertxMySQLClient, lLogger);

            case FILE_VERSION_ID_GEN_REG_REQ:
                return NameNodeUtils.getFileVersionId((FileVersionIdGenerateAndRegisterReq) lRequestPayload, lVertxMySQLClient, lLogger);

            default:
                return new StorageClsMetaErrorRsp("Sent Unknown PayloadType",  lRequestPayload);
        }
    }
}
