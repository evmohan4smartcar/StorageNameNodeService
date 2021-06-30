package in.intellicar.layer5.service.namenode.server;

import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaPayload;
import in.intellicar.layer5.beacon.storagemetacls.payload.SplitBucketReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.IBucketRelatedIdInfoProvider;
import in.intellicar.layer5.beacon.storagemetacls.service.common.props.BucketInfo;
import in.intellicar.layer5.utils.sha.SHA256Item;
import in.intellicar.layer5.utils.sha.SHA256Utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class BucketModel implements IPayloadBucketInfoProvider, IBucketEditor{
    private List<BucketInfo> _buckets;
    private BucketInfo _newSplitBucket;
    private AtomicBoolean _isSplitInProgress;

    public BucketModel()
    {
        _buckets = new ArrayList<>();
        _newSplitBucket = null;
        _isSplitInProgress = new AtomicBoolean(false);
    }

    @Override
    public BucketInfo getBucketForPayload(StorageClsMetaPayload lPayload) {
        if(lPayload instanceof IBucketRelatedIdInfoProvider)
        {
            return getMatchingBucketForId(((IBucketRelatedIdInfoProvider)lPayload).getIdReleatedToBucket().toHex());
        }
        else if(lPayload instanceof SplitBucketReq)
        {
            return null;
        }
        return null;
    }

    public BucketInfo getMatchingBucketForId(String lIdToMatch)
    {
        BucketInfo returnValue = null;
        for(BucketInfo curBucket : _buckets)
        {
            if(isBucketMatched(lIdToMatch, curBucket))
            {
                returnValue = curBucket;
                break;
            }
        }
        return returnValue;
    }

    /**
     * Considers lSplitId=null case as split end notification
     * @param lSplitId
     * @return false, if the splitId provided doesn't belong to any of the buckets of this server
     */
    @Override
    public boolean setSplitId(SHA256Item lSplitId) {
        boolean returnValue = false;
        if(lSplitId == null)//considering null case as split end notification
        {
            _newSplitBucket = null;
            _isSplitInProgress.set(false);
            returnValue = true;//not needed, as we won't be checking it on split end notification
        }
        for(BucketInfo curBucket : _buckets)
        {
            if(isBucketMatched(lSplitId.toHex(), curBucket))
            {
                _newSplitBucket = new BucketInfo(SHA256Utils.getOneAddedHash(lSplitId).toHex(), curBucket.endBucket.toHex());
                _isSplitInProgress.set(true);
                returnValue = true;
                break;
            }
        }
        return returnValue;
    }

    @Override
    public boolean doesBelongToNewSplitBucket(StorageClsMetaPayload lPayload) {
        boolean returnValue = false;
        if(_isSplitInProgress.get() && lPayload instanceof IBucketRelatedIdInfoProvider)
        {
            returnValue = isBucketMatched(((IBucketRelatedIdInfoProvider) lPayload).getIdReleatedToBucket().toHex(), _newSplitBucket);
        }
        return returnValue;
    }

    @Override
    public void addBucket(BucketInfo lBucket) {
        _buckets.add(lBucket);
    }

    @Override
    public void removeBucket(BucketInfo lBucket) {
        _buckets.remove(lBucket);
    }

    @Override
    public void splitBucket(SHA256Item lSplitId)
    {
        //TODO:: lock need to be used around list
        //null check isn't needed as it's called by bucketmanager who already taken care of spliting
        BucketInfo matchedBucket = getMatchingBucketForId(lSplitId.toHex());
        _buckets.remove(matchedBucket);
        BucketInfo newBucket = new BucketInfo(SHA256Utils.getOneAddedHash(lSplitId).toHex(), matchedBucket.endBucket.toHex());
        matchedBucket.endBucket = lSplitId;
        _buckets.add(matchedBucket);
        _buckets.add(newBucket);
    }

    private boolean isBucketMatched(String lIdToMatch, BucketInfo lBucket)
    {
        return lIdToMatch.compareToIgnoreCase(lBucket.startBucket.toHex()) >= 0
                && lIdToMatch.compareToIgnoreCase(lBucket.endBucket.toHex()) <= 0;
    }

}
