const { map, mergeMap, tap, retry } = require("rxjs/operators");
const { of, Observable, defer, forkJoin, from } = require("rxjs");


const MongoClient = require('mongodb').MongoClient;
const urlCurrent = 'mongodb://localhost:27017';
const urlHistory = 'mongodb://localhost:27018';
const COLLECTION_NAME = 'Orderbox';
const dbNameHistory = 'historical_1910';
const dbNameCurrent = 'logistic-operation';

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
        'retrievalId': 'c91e13ef-8726-4b03-9a41-a171045fdca6'
    };
    defer(() => mongoDBHistory.collection(COLLECTION_NAME).find(query).toArray())
    .pipe(
        mergeMap(orderboxes => from(orderboxes)),
        mergeMap(orderbox => {

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
                //auditAnswer: orderbox.auditAnswer,
                stowageName: orderbox.stowageName,
                stowageId: orderbox.stowageId,
                deliveryId: orderbox.deliveryId,
            };

            if(orderbox.deliveryProof){
                updateOperation['deliveryProof'] = orderbox.deliveryProof;
            }

            if(orderbox.deliveries){
                updateOperation['deliveries'] = orderbox.deliveries;
            }

            return defer(() => mongoDBCurrent.collection(COLLECTION_NAME).findOneAndUpdate({_id: orderbox._id}, {$set: updateOperation}, 
            { returnOriginal: false }))
            .pipe(
                map(result => result && result.value ? result.value : undefined)
            )
        })

    )
    .subscribe(result => {
        console.log('Result => ', result);
    }, error => {
        console.log('Error subscription => ', error);
    },
    () => {
        console.log('Completed');
    });
};



