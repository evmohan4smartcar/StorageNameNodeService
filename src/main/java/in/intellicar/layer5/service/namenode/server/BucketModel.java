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

public class BucketModel implements IPayloadBucketInfoProvider, IBucketEditor{
    private List<BucketInfo> _buckets;
    private BucketInfo _newSplitBucket;
    private Object _newBucketLock;
    public BucketModel()
    {
        _buckets = new ArrayList<>();
        _newSplitBucket = null;
        _newBucketLock = new Object();
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

    @Override
    public void setSplitId(SHA256Item lSplitId) {
        if(lSplitId == null)//considering null case as split end notification
        {
            synchronized (_newBucketLock) {
                _newSplitBucket = null;
            }
        }
        for(BucketInfo curBucket : _buckets)
        {
            if(isBucketMatched(lSplitId.toHex(), curBucket))
            {
                synchronized (_newBucketLock) {
                    _newSplitBucket = new BucketInfo(SHA256Utils.getOneAddedHash(lSplitId).toHex(), curBucket.endBucket.toHex());
                }
                break;
            }
        }
    }

    @Override
    public boolean doesBelongToNewSplitBucket(StorageClsMetaPayload lPayload) {
        boolean returnValue = false;
        boolean isSplitGoingOn = false;
        synchronized (_newBucketLock) {
            isSplitGoingOn = (_newSplitBucket != null);
            returnValue = false;
        }
        if(isSplitGoingOn && lPayload instanceof IBucketRelatedIdInfoProvider)
        {
            synchronized (_newBucketLock) {//TODO:: need to remove this mutex; Mutex making it all look like a mess
                returnValue = isBucketMatched(((IBucketRelatedIdInfoProvider) lPayload).getIdReleatedToBucket().toHex(), _newSplitBucket);
            }
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
