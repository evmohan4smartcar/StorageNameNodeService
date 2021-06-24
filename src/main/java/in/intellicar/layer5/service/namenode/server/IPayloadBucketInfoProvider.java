package in.intellicar.layer5.service.namenode.server;

import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaPayload;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.IBucketRelatedIdInfoProvider;
import in.intellicar.layer5.beacon.storagemetacls.service.common.props.BucketInfo;
import in.intellicar.layer5.utils.sha.SHA256Item;

public interface IPayloadBucketInfoProvider
{
    public BucketInfo getBucketForPayload(StorageClsMetaPayload lPayload);
    public BucketInfo getMatchingBucketForId(String lIdToMatch);
    public void setSplitId(SHA256Item lSplitId);
    public boolean doesBelongToNewSplitBucket(StorageClsMetaPayload lPayload);

}
