const moment = require('moment');
const mongoose = require('mongoose')
const winston = require('winston');
require('dotenv').config();



const dbConnection = {

    useNewUrlParser: true,
    useUnifiedTopology: true,


};

let stop, targetedField, tempLog, elem, allLogs, totalDeletedDocuments = 0,
    fieldNames, collections;


async function deleteOneMonthOldRecords(maxDeleteAtOnce, intervalTime) {

    try {

        console.log('Process start');

        console.log(
            `Mongo string`,
            `mongodb://${process.env.ANALYTICS_MONGO_URI}?authSource=admin`,
            process.env.AUTH_SOURCE
        );
        
        await mongoose.connect(
            `mongodb://${process.env.ANALYTICS_MONGO_URI}`,
            dbConnection
        );

        collections = await (
            await mongoose.connection.db.listCollections().toArray()
        ).map((collection) => collection.name);

        console.log(collections);
        console.log(`Total Collections are ${collections.length}`);

        fieldNames = ["createdAt", "updatedAt", "userLogInTime"];

        const promiseCollections = await Promise.all(collections);
        console.log(promiseCollections)
        const totalCollections = promiseCollections.length;

        const colData = async () => {
            const timeout = ms => new Promise(res => setTimeout(res, ms));

            (async () => {

                var i = 0;

                while (true) {

                    await timeout(intervalTime*1000);

                    let collectionName = promiseCollections[i++]

                    let model = mongoose.model(collectionName, new mongoose.Schema({}, {
                        strict: false
                    }));

                    try {
                        tempLog = await model.findOne().lean();
                        for (elem of fieldNames) {
                            if (elem in tempLog) {
                                targetedField = elem;
                            }
                        }

                    } catch (err) {
                        console.log(`No Documents have been found in ${collectionName}`);
                    }

                    try {
                        allLogs = await model.find({

                            [targetedField]: {
                                $lte: moment().subtract(1, 'month').toDate()
                            }

                        }).lean().select({
                            _id: 1
                        });
                    } catch (err) {
                        console.log(`Cannot able to find data ${err}`);
                    }

                    let allLogsLen;

                    if (tempLog) {

                        allLogsLen = allLogs.length;
                        console.log(`Total Documents in ${collectionName} collection ${allLogsLen}`);
                    }

                    if (tempLog) {
                        
                        if (allLogsLen > maxDeleteAtOnce) {
                            let start = 0, end = maxDeleteAtOnce, targetedIds = []
                
                            allLogs.forEach(element => {
                                targetedIds.push(element._id)
                            });
                            
                            let deletedCount = 0;

                            while(allLogsLen >= 0) {
                                await timeout(intervalTime*1000);
                                console.log(end)
                                let deletingCall = await model.deleteMany({
                                    _id: { $in: targetedIds.slice(start, end) }
                                })
                
                                start += maxDeleteAtOnce;
                
                                (allLogsLen > maxDeleteAtOnce) ? end += maxDeleteAtOnce : end += allLogsLen;
                
                                allLogsLen -= maxDeleteAtOnce;

                                deletedCount += deletingCall.deletedCount;
                            }

                            console.log(`total records deleted from ${collectionName} are : ${deletedCount}`);
                            console.log(`deletion process for ${collectionName} is done.`);

                        } else {
                            await timeout(intervalTime*1000);
                            let deletingCall = await model.deleteMany({
                                [targetedField]: {
                                    $lte: moment().subtract(1, 'month').toDate()
                                },
                            })
                            console.log(`total records deleted from ${collectionName} are : ${deletingCall.deletedCount}`);
                            console.log(`deletion process for ${collectionName} is done.`)
                            
                        }
                    }
                    // temination logic
                    if (i >= totalCollections) {
                        break;
                    }

                }
                console.log("Process finished ");
            })();
        }
        colData();
    } catch (err) {
        console.log(err);
    }
}







// deleteOneMonthRecords(maxDeleteAtOnce, intervalTime in seconds);
deleteOneMonthOldRecords(100, 4);





const logger = winston.createLogger({
    level: 'info',
    format: winston.format.json(),
    defaultMeta: {
        service: 'user-service'
    },
    transports: [

        new winston.transports.File({
            filename: 'error.log',
            level: 'error',

        }),
        new winston.transports.File({
            filename: 'combined.log',

        }),

    ],
});




if (process.env.NODE_ENV !== 'production') {



    logger.add(new winston.transports.Console({
        format: winston.format.simple(),
    }));


}
