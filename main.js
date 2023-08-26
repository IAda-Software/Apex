const cluster = require("cluster");
const mongo = require("mongodb");
const https = require("https");
const http = require("http");
const os = require("os");
const fs = require("fs");

const xsync = require("./library/xsync.js")
const server = require("./library/server.js")

const serviceManager = require("./library/serviceManager.js");

function each(arr, cb) {
    if (!arr['length']) {
        for (var i in arr) {

            if (cb(i, arr[i]) === true) return true;
        }
    } else {
        for (var i = 0; i < arr.length; ++i) {
            if (cb(i, arr[i]) === true) return true;
        }
    }
    return false;
}

function connectorObject(){
    return this;
}
connectorObject.prototype.connectDatabase=function(connection,callback){
    connection.connect()
        .then(function(db){
            callback(null,db)
        })
        .catch(function(e){
            callback(e);
        })
}

var connectorInterface = new connectorObject();

const numCPUs = os.availableParallelism();

var settings = {};

//! Load the global settings file
try {
    settings = JSON.parse(fs.readFileSync("./settings.json"));
} catch (e){
    console.log("Could not parse settings file");
    console.log(e);
}

var dataConnectors = [];

//! Build the storage directory
const storagePath = (!settings['storage'])?"./www":settings.storage;

function masterMessageHandler(message){
    switch(message.action){
        case "restart": {
            //! Reload settings file
            settings = JSON.parse(fs.readFileSync("./settings.json"));
            //! Kill all workers
            for (var id in cluster.workers) {
                cluster.workers[id].kill();
            }
            //! They should automatically start up again
        } break;
        default: {} break;
    }
}

//! Check if this process is the master or worker
if (cluster.isPrimary) {
    /**This is the main cluster thread*/
    var services = null;

    // Fork workers.
    for (let i = 0; i < numCPUs; i++) {
        var worker = cluster.fork();

        worker.on('message',masterMessageHandler);
        console.log("Worker started")
    }

    cluster.on('exit', (worker, code, signal) => {
        //! Start another worker
        var worker = cluster.fork();

        worker.on('message',masterMessageHandler);
        console.log("Worker restarted")
    });

    //! We have database connectors
    const client = new mongo.MongoClient(settings.databases[0].uri,  {
            serverApi: {
                version: mongo.ServerApiVersion.v1,
                strict: true,
                deprecationErrors: true,
            }
        }
    );

    client.connect()
        .then(function(db){
            console.log("Connected to database successfully!");

            //! Create a new instance of the service manager
            services = new serviceManager({
                settings:settings,
                database:db
            });
        })
        .catch(function(e){
            console.log("Could not connect to database");
        })
} else {
    //! This is for worker threads
    var service;

    var options = {
        path:storagePath
    };

    //! Copy some options from our settings file
    options["enableDatabaseSessions"] = (!settings.enableDatabaseSessions)?false:settings.enableDatabaseSessions;
    options["enableSocketSecurity"] = (!settings.enableSocketSecurity)?false:settings.enableSocketSecurity;
    options["sessionsDatabase"] = (!settings.sessionsDatabase)?null:settings.sessionsDatabase;
    options["standardPort"] = (!settings.port)?8080:settings.port;

    //! Check to see if we will use one or more databases
    if(settings.databases.length > 0){

        var sync = new xsync();

        each(settings.databases,function(i,iv){
            //! Create a new Mongo Client instance
            var newConnection = new mongo.MongoClient(iv.uri, {
                serverApi: {
                    version: mongo.ServerApiVersion.v1,
                    strict: true,
                    deprectionErrors: true
                }
            }) 

            sync
                .then(connectorInterface,"connectDatabase",[newConnection],function(e,db){
                    if(!e){
                        console.log("Connected successfully");

                        dataConnectors.push({
                            client:newConnection,
                            database:db
                        });
                    } else {
                        //! Could not connect to the database instance
                        console.log("Could not connect on client:");
                        console.log("\t", iv.uri);
                        console.log("Reason:");
                        console.log("\t", e);
                    }
                })
        }); //! END DATABASE LOOP

        //! When the sync chain is completed
        sync.done(function(e){
            console.log("Connected to", dataConnectors.length, "databases");

            //! Add them to our options object
            options['connectors'] = dataConnectors;

            //! Use the first database connection as our core connector
            options['database'] = dataConnectors[0].database;
            options['mongoClient'] = dataConnectors[0].client;

            service = new server(options);
        })

        


    } //! END DATABASE SPECIFIC CODE


} //! END WORKER SPECIFIC CODE