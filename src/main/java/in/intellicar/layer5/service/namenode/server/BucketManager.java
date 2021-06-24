package in.intellicar.layer5.service.namenode.server;

import com.fasterxml.jackson.databind.JsonNode;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaPayload;
import in.intellicar.layer5.beacon.storagemetacls.payload.SplitBucketReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.IBucketRelatedIdInfoProvider;
import in.intellicar.layer5.beacon.storagemetacls.service.common.mysql.MySQLQueryHandler;
import in.intellicar.layer5.beacon.storagemetacls.service.common.props.BucketInfo;
import in.intellicar.layer5.beacon.storagemetacls.service.common.props.MySQLProps;
import in.intellicar.layer5.service.namenode.mysql.NameNodePayloadHandler;
import in.intellicar.layer5.utils.sha.SHA256Item;
import in.intellicar.layer5.utils.sha.SHA256Utils;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class BucketManager extends Thread {

    //private List<Thread> _bucketThreads;
    //private Map<BucketInfo, MySQLQueryHandler> _bucketThreadMap;
    private Map<String, MySQLQueryHandler> _bucketThreadMap;//map with object as key would give me tough time, hence changed to string
    private MySQLProps _mySqlProps;//is used only to load buckets from initial config values. There after it maintains it's own list of buckets array.
    private Vertx _vertx;
    private LinkedBlockingDeque<Message<StorageClsMetaPayload>> _eventQueue;
    private EventBus _eventBus;
    private static String CONSUMER_ADDRESS_PREFIX = "/mysqlqueryhandler";
    private String _scratchDir;
    private Logger _logger;
    private AtomicBoolean _stop;
    private IBucketEditor _bucketModel;
    private IPayloadBucketInfoProvider _bucketInfoProvider;
    private IBucketsConfigUpdater _configUpdater;

    public BucketManager(Vertx lVertx, String lScratchDir, MySQLProps lMySQLProps, IBucketEditor lBucketEditor, IPayloadBucketInfoProvider lBucketInfoProvider, IBucketsConfigUpdater lConfigUpdater, Logger lLogger)
    {
        _mySqlProps = lMySQLProps;
        _vertx = lVertx;
        _bucketThreadMap = new HashMap<>();
        //_bucketThreads = new ArrayList<>();
        _eventQueue = new LinkedBlockingDeque<>();
        _logger = lLogger;
        _scratchDir = lScratchDir;
        _stop = new AtomicBoolean(false);
        _bucketModel = lBucketEditor;
        _bucketInfoProvider = lBucketInfoProvider;
        _configUpdater = lConfigUpdater;
    }

    public void init()
    {
        Iterator<BucketInfo> it = _mySqlProps.buckets.listIterator();
        while(it.hasNext())
        {
            BucketInfo curBucket = it.next();
            String consumerAddress = CONSUMER_ADDRESS_PREFIX + "/" + curBucket.startBucket.toHex() + "/" + curBucket.endBucket.toHex();
            //String consumerAddress = CONSUMER_ADDRESS_PREFIX;
            MySQLQueryHandler<StorageClsMetaPayload> mysqlQueryHandler = new MySQLQueryHandler(_vertx, _scratchDir, _mySqlProps,
                    new NameNodePayloadHandler(), consumerAddress, _logger);
            mysqlQueryHandler.init();
            mysqlQueryHandler.start();
            _bucketThreadMap.put(consumerAddress, mysqlQueryHandler);
            _bucketModel.addBucket(curBucket);
        }

        _eventBus = _vertx.eventBus();
        _eventBus.consumer("/mysqlqueryhandler", new Handler<Message<StorageClsMetaPayload>>() {
            @Override
            public void handle(Message<StorageClsMetaPayload> event) {
//                logger.info("Received msg:" + event.body().toJson(logger));
//                handleMySQLQuery(event);
//                eventQueue.add(event);
                _eventQueue.add(event);
            }
        });
        _stop.set(false);
    }

    public void stopService()
    {
        _stop.set(true);
        Collection<MySQLQueryHandler> bucketThreads= _bucketThreadMap.values();
        for(MySQLQueryHandler bThread : bucketThreads)
        {
            bThread.stopService();
        }
        bucketThreads.clear();
    }

    @Override
    public void run() {
        super.run();
        try {
            while (!_stop.get()) {
                Message<StorageClsMetaPayload> event = _eventQueue.poll(1000, TimeUnit.MILLISECONDS);
//                Future<StorageClsMetaPayload> future = Future.future();

                if (event != null && event.body() instanceof SplitBucketReq) {

                    SHA256Item splitId = ((SplitBucketReq)event.body()).splitAt;
                    String splitIdString = splitId.toHex();

                    //Notify connHandler so that it takescare of new requests belonging to new bucket
                    //TODO:: yet to handle the case when the connection established after notification and while in process of split
                    //_eventBus.request(NameNodeConnHandler.SPLIT_END_NOTIFICATION_ADDRESS, splitIdString);
                    _bucketInfoProvider.setSplitId(splitId);// ConnHandlers call method on bucketInfoProvider to know if the payload belongs to new split or not;hence no need of above notification

                    //use shell script to split the bucket
                    executeSplit(splitIdString);

                    //updating buckets and their sqlHandlers wrt split
                    updateBucketsAndBucketRelatedThreadOnSplit(splitId);

                    //Update configFile
                    _configUpdater.splitBucketAt(splitIdString);

                    //notification without idString is like saying that the split is done.
                    _eventBus.request(NameNodeConnHandler.SPLIT_END_NOTIFICATION_ADDRESS, "");
                    _bucketInfoProvider.setSplitId(null);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private String getBucketRelatedIdStringForPayload(StorageClsMetaPayload lRequestPayload)
    {
        String returnValue = "";
       if(lRequestPayload instanceof IBucketRelatedIdInfoProvider)
       {
           SHA256Item idToCheck = ((IBucketRelatedIdInfoProvider)lRequestPayload).getIdReleatedToBucket();
           returnValue = idToCheck.toHex();
       }
       return returnValue;
    }

    private boolean isBucketMatched(String lIdToMatch, BucketInfo lBucket)
    {
        return lIdToMatch.compareToIgnoreCase(lBucket.startBucket.toHex()) > 0
                && lIdToMatch.compareToIgnoreCase(lBucket.endBucket.toHex()) <= 0;
    }

    private String getConsumerAddressForBucket(BucketInfo lBucket)
    {
        return CONSUMER_ADDRESS_PREFIX + "/" + lBucket.startBucket.toHex() + "/" + lBucket.endBucket.toHex();
    }

    private String getVertexConsumerAddress(StorageClsMetaPayload lRequestPayload)
    {
        String idToCheck = getBucketRelatedIdStringForPayload(lRequestPayload);
        String returnValue = "";
        Iterator<BucketInfo> it = _mySqlProps.buckets.listIterator();
        while(it.hasNext())
        {
            BucketInfo curBucket = it.next();
            if(isBucketMatched(idToCheck, curBucket))
            {
                returnValue = getConsumerAddressForBucket(curBucket);
                break;
            }
        }
        return returnValue;
    }

    private void executeSplit(String lSplitId) {
        try {

            // Run a shell script
            Process process = Runtime.getRuntime().exec(_scratchDir + "/scripts/split.sh");

            StringBuilder output = new StringBuilder();

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()));

            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line + "\n");
            }

            int exitVal = process.waitFor();
            if (exitVal == 0) {
                _logger.info("Success!");
                _logger.info(String.valueOf(output));
            } else {
                _logger.info("Print script failed");
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void updateBucketsAndBucketRelatedThreadOnSplit(SHA256Item lSplitId)
    {
        String splitIdString = lSplitId.toHex();
        //updating current bucket and it's sqlHandler
        BucketInfo currentBucket = _bucketInfoProvider.getMatchingBucketForId(splitIdString);
        String currentBucketConsumerAddress = CONSUMER_ADDRESS_PREFIX + "/" + currentBucket.startBucket.toHex() + "/" + currentBucket.endBucket.toHex();
        MySQLQueryHandler currentSQLHandler = _bucketThreadMap.get(currentBucketConsumerAddress);
        String updatedCurrentConsumerAddress = CONSUMER_ADDRESS_PREFIX + "/" + currentBucket.startBucket.toHex() + "/" + splitIdString;
        _bucketThreadMap.remove(currentBucketConsumerAddress);
        _bucketThreadMap.put(updatedCurrentConsumerAddress, currentSQLHandler);
        //registering new consumer address
        currentSQLHandler.registerConsumerAddress(updatedCurrentConsumerAddress);

        //adding new split and it's sqlHandler
        String newBucketConsumerAddress = CONSUMER_ADDRESS_PREFIX + "/" + SHA256Utils.getOneAddedHash(lSplitId).toHex() + "/" + currentBucket.endBucket.toHex();
        MySQLQueryHandler<StorageClsMetaPayload> mysqlQueryHandler = new MySQLQueryHandler(_vertx, _scratchDir, _mySqlProps,
                new NameNodePayloadHandler(), newBucketConsumerAddress, _logger);
        mysqlQueryHandler.init();
        mysqlQueryHandler.start();
        _bucketThreadMap.put(newBucketConsumerAddress, mysqlQueryHandler);

        //update BucketModel
        _bucketModel.splitBucket(lSplitId);

        //de-registering old consumer of current bucket
        currentSQLHandler.updateEventConsumer();
    }

}


