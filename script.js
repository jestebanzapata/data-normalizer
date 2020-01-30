const { map, mergeMap, tap, retry } = require("rxjs/operators");
const { of, Observable, defer, forkJoin, from } = require("rxjs");


const MongoClient = require('mongodb').MongoClient;
const urlCurrent = 'mongodb://localhost:27017';
const urlHistory = 'mongodb://localhost:27018';
const COLLECTION_NAME = 'Orderbox';
const dbNameHistory = 'historical_2001';
const dbNameCurrent = 'logistic-operation';

const PASSED_AUDIT_ENU = {
    0: "NOT_REQUIRED",
    1: "COMPLETED_AUDIT",
    2: "NOT_COMPLETED_AUDIT",
    3: "NOT_INVENTORY"
  }

MongoClient.connect(urlHistory, function(err, dbHistory) {

 if(dbHistory){
    this.mongoDBHistory = dbHistory.db(dbNameHistory);

    MongoClient.connect(urlCurrent, function(err, dbCurrent) {
        
        if(err){
            console.log('Err => ', err);
        }
        if(dbCurrent){
            this.mongoDBCurrent = dbCurrent.db(dbNameCurrent);

            if(this.mongoDBHistory && this.mongoDBCurrent){
                start(mongoDBHistory, mongoDBCurrent);
            }
            
        }
    });

 }
});

const start = (mongoDBHistory, mongoDBCurrent) => {
    const query = {
        //'retrievalId': 'c91e13ef-8726-4b03-9a41-a171045fdca6'
        'lastModificationTimestamp': { $gte: 1580274000000 },
        'evicted': true
        //_id: '3d3a0b60-f7ea-4084-b80d-58ce46a40326-2001'
    };
    defer(() => mongoDBCurrent.collection(COLLECTION_NAME).find(query).toArray())
    .pipe(
        tap(result => {
            console.log(result.length);
        }),
        mergeMap(orderboxes => from(orderboxes)),
        mergeMap(orderbox => {
            //console.log('orderbox => ', orderbox);
            const updateOperation = {
                stateChanges: orderbox.stateChanges,
                currentState: orderbox.currentState,
                delayed: orderbox.delayed,
                delivered: orderbox.delivered,
                evicted: orderbox.evicted,
                lastModificationTimestamp: orderbox.lastModificationTimestamp,
                lastStateChangeTimestamp: orderbox.lastStateChangeTimestamp,
                shopper: orderbox.shopper,
                routeId: orderbox.routeId,
                routeName: orderbox.routeName,
                distributionCenterDocumentId: orderbox.distributionCenterDocumentId,
                distributionCenterId: orderbox.distributionCenterId,
                distributionCenterName: orderbox.distributionCenterName,
                expirationTimestamp: orderbox.expirationTimestamp,
                promiseTime: orderbox.promiseTime,
                auditAnswer: PASSED_AUDIT_ENU[orderbox.auditAnswer || 0],
                stowageName: orderbox.stowageName,
                stowageId: orderbox.stowageId,
                deliveryId: orderbox.deliveryId,
                contentQty: orderbox.contentQty,
                audit: orderbox.audit,
                barcode: orderbox.barcode,
                brand: orderbox.brand,            
                businessId: orderbox.businessId,
                campaign: orderbox.campaign,
                contentDesc: orderbox.contentDesc,
                generatorBoxId: orderbox.generatorBoxId,
                generatorDispatch: orderbox.generatorDispatch,
                generatorDispatchId: orderbox.generatorDispatchId,
                generatorDocumentId: orderbox.generatorDocumentId,
                generatorExtras: orderbox.generatorExtras,
                generatorId: orderbox.generatorId,
                generatorInvoiceId: orderbox.generatorInvoiceId,
                generatorName: orderbox.generatorName,
                generatorOrderId: orderbox.generatorOrderId,
                logisticOperatorCode: orderbox.logisticOperatorCode,
                logisticOperatorId: orderbox.logisticOperatorId,
                logisticOperatorName: orderbox.logisticOperatorName,
                orderId: orderbox.orderId,
                orderboxesCount: orderbox.orderboxesCount,
                productType: orderbox.productType,
                shopper: orderbox.shopper,
                deliveryLicensePlate: orderbox.deliveryLicensePlate,
                retrievalId: orderbox.retrievalId,
                timestamp: orderbox.timestamp
            };

            if(orderbox.deliveryProof){
                updateOperation['deliveryProof'] = orderbox.deliveryProof;
            }

            if(orderbox.deliveries){
                updateOperation['deliveries'] = orderbox.deliveries;
            }

            return defer(() => mongoDBHistory.collection(COLLECTION_NAME).findOneAndUpdate({_id: orderbox._id}, {$set: updateOperation}, 
            { returnOriginal: false, upsert: true }))
            .pipe(
                map(result => result && result.value ? result.value : undefined)
            )
        })

    )
    .subscribe(result => {
        //console.log('Result => ', result);
    }, error => {
        console.log('Error subscription => ', error);
    },
    () => {
        console.log('Completed');
    });
};



