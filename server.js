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


async function deleteOneMonthOldRecords(maxDeleteAtOnce) {

    try {


        console.log('Process start');

        await mongoose.connect('mongodb://localhost:27017/transaction', dbConnection);



        collections = await (
            await mongoose.connection.db.listCollections().toArray()
        ).map((collection) => collection.name);

        console.log(collections);
        console.log(`Total Collections are ${collections.length}`);



        fieldNames = ['bucket_start_date', 'bucket_end_date'];

        const promiseCollections = await Promise.all(collections);






        const colData = async () => {
            const timeout = ms => new Promise(res => setTimeout(res, ms));

            (async () => {
                var i = 0;
                while (true) {
                    await timeout(1000);
                    console.log(promiseCollections[i++]);
                    if (i >= totalCollections) {
                        break;
                    }
                }
                console.log("done");
            })();

        //     for (let collectionName of promiseCollections) {

        //         let model = mongoose.model(collectionName, new mongoose.Schema({}, {
        //             strict: false
        //         }));

        //         try {
        //             tempLog = await model.findOne().lean();


        //             for (elem of fieldNames) {
        //                 if (elem in tempLog) {
        //                     targetedField = elem;
        //                 }
        //             }

        //         } catch (err) {

        //             console.log(`No Documents have been found in ${collectionName}`);
        //         }




        //         try {
        //             allLogs = await model.find({

        //                 [targetedField]: {
        //                     $lte: moment().subtract(1, 'month').toDate()
        //                 }

        //             }).lean().select({
        //                 _id: 1
        //             });
        //         } catch (err) {
        //             console.log(`Cannot able to find data ${err}`);
        //         }



        //         let allLogsLen;

        //         if (tempLog) {

        //             allLogsLen = allLogs.length;
        //             console.log(`Total Documents in ${collectionName} collection ${allLogsLen}`);
        //         }

        //         if (tempLog) {

        //             if (allLogsLen > maxDeleteAtOnce) {
        //                 let start = 0,
        //                     end = maxDeleteAtOnce

        //                 let ids = allLogs.map((elem) => elem._id);


        //                 const documentsDeletion = async () => {




        //                     try {
        //                         const deleteDocuments = await model.deleteMany({
        //                             _id: {
        //                                 $in: ids.slice(start, end)
        //                             }
        //                         })



        //                         if (deleteDocuments.deletedCount !== maxDeleteAtOnce) {


                                 
        //                             totalDeletedDocuments = totalDeletedDocuments + deleteDocuments.deletedCount;
                              
        //                             if(totalDeletedDocuments){
        //                                 console.log(`Total Documents have been deleted in ${collectionName} collection ${totalDeletedDocuments}`);
                                        
                                      

        //                                 const stopInterval = () =>
        //                                 {
        //                                     clearInterval(stop);
        //                                     console.log('Interval stops...');
        //                                 }
        //                                 stopInterval();
                                      
        //                             }
                                   
                                  


        //                         } else {

                                    
        //                             totalDeletedDocuments = totalDeletedDocuments + deleteDocuments.deletedCount;

        //                             start += maxDeleteAtOnce;
        //                             end += maxDeleteAtOnce;





        //                         }

        //                     } catch (err) {
        //                         console.log(`Cannot able to delete the data ${err}`);
        //                     }

        //                 }


        //                 stop = setInterval(documentsDeletion, 800);




        //             } else {
        //                 let start = 0,
        //                     end = 30;

        //                 let ids = allLogs.map((elem) => elem._id);
        //                 let totalDeletedDocuments = 0;

        //                 const documentsDeletion2 = async () => {


        //                     try {
        //                         const deleteDocuments = await model.deleteMany({
        //                             _id: {
        //                                 $in: ids.slice(start, end)
        //                             }
        //                         })

        //                         if (deleteDocuments.deletedCount !== 30) {


        //                             totalDeletedDocuments = totalDeletedDocuments + deleteDocuments.deletedCount;
                                  
        //                             if(totalDeletedDocuments){
        //                                 console.log(`Total Documents have been deleted in ${collectionName} collection ${totalDeletedDocuments}`);
                                        
                                      

        //                                 const stopInterval = () =>
        //                                 {
        //                                     clearInterval(stop);
        //                                     console.log('Interval stops...');
        //                                 }
        //                                 stopInterval();
                                      
        //                             }



        //                         } else {


        //                             console.log('deletingggg wait.......');

        //                             totalDeletedDocuments = totalDeletedDocuments + deleteDocuments.deletedCount;

        //                             start += 30;
        //                             end += 30;





        //                         }
        //                     } catch (err) {
        //                         console.log(`Cannot able to delete the data ${err}`);
        //                     }



        //                 }

        //                 stop = setInterval(documentsDeletion2, 800);

        //             }

        //         }


        //     };
        }
        colData();


    } catch (err) {
        console.log(err);
    }
}




deleteOneMonthOldRecords(100);





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
