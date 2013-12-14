/*
 * Copyright 2013 Jive Software
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */


var q = require('q');
var jive = require('jive-sdk');
var flat = require('flat');
var ArrayStream = require('stream-array');

function sanitize(key) {
    return key.replace('.', '_');
}

function hydrate(row) {
    var toUnflatten = {};
    for (var dataKey in row) {

        if (row.hasOwnProperty(dataKey)) {
            var value = row[dataKey];
            if (value ) {
                if ( value.indexOf && value.indexOf('<__@> ') == 0 ) {
                    value = value.split('<__@> ')[1];
                    value = JSON.parse(value);
                }
                toUnflatten[dataKey] = value;
            }
        }
    }

    var obj = flat.unflatten(toUnflatten, {'delimiter': '_'});
    delete obj[""];
    return obj;
}

module.exports = function(serviceConfig) {
    var databaseUrl;

    // setup database url
    if (serviceConfig ) {
        databaseUrl = serviceConfig['databaseUrl'];
    }

    if ( !databaseUrl ) {
        databaseUrl = 'pg://postgres:postgres@localhost:5432/mydb';
    }

    jive.logger.info("****************");
    jive.logger.info("Redis configured");
    jive.logger.info("****************");

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Private

    // driver
    var postgres = require('pg');
    var db = new postgres.Client(databaseUrl);

    var sync = undefined;
    var schema = {};
    var toSync = {};
    if ( serviceConfig['schema'] ) {
        toSync = serviceConfig['schema'];
        if ( toSync ) {
            for ( var k in toSync ) {
                if (toSync.hasOwnProperty(k) ) {
                    var value = toSync[k];
                    delete toSync[k];
                    toSync[k.toLowerCase()] = value;
                }
            }
        }
    }

    db.connect(function(err) {
        if(err) {
            jive.logger.error(err);
            return process.exit(-1);
        }

        var Sync = require("sql-ddl-sync").Sync;
        sync = new Sync({
            dialect : "postgresql",
            db      : db,
            debug   : function (text) {
                jive.logger.info("> %s", text);
            }
        });
    });

    var query = function(sql) {
        var p = q.defer();
        jive.logger.debug(sql);
        db.query(sql, function(err, result) {
            if(err) {
                p.reject(err);
            }
            p.resolve(result);
        });

        return p.promise;
    };

    var tableExists = function(table) {
        return query("select * from pg_tables where tablename='" + table + "'").then( function(r) {
            return r && r.rowCount > 0;
        }, function(e) {
            return q.reject(e);
        });
    };

    var dropTable = function(table) {
        return query("drop table \"" + table + "\"").then( function(r) {
            return r;
        }, function(e) {
            return q.reject(e);
        });
    };

    var syncTable = function( table, dropIfExists, force ) {
        var p = q.defer();

        var tableName = table['tableName'];
        tableName = tableName.replace('"','');
        tableName = tableName.toLowerCase();

        var tableAttrs = table['attrs'];
        if ( !tableAttrs['_id'] ) {
            tableAttrs['_id'] = { type: "text", required: true };
        }

        // sanitize column names
        for ( var key in tableAttrs ) {
            if ( tableAttrs.hasOwnProperty(key) ) {
                var value = tableAttrs[key];
                if ( key.indexOf('.') > -1 ) {
                    delete tableAttrs[key];
                    key = sanitize(key);
                    tableAttrs[key] = value;
                }
                schema[tableName] = tableAttrs;
            }
        }

        tableExists(tableName).then( function(exists) {

            if ( !exists || force ) {

                sync.defineCollection(tableName, tableAttrs);

                sync.sync(function (err) {
                    if (err) {
                        jive.logger.error("> Sync Error", err);
                        p.reject(err);
                    } else {
                        jive.logger.info("> Sync Done", tableName );
                        p.resolve();
                    }
                });

            } else if (dropIfExists ) {
                dropTable(tableName).then( function() {
                    syncTable(table, false).then( function() {
                        p.resolve();
                    }, function(e) {
                        p.reject(e);
                    })
                });
            } else {
                jive.logger.debug("table already exists");
                p.resolve();
            }
        });

        return p.promise;
    };

    var expandIfNecessary = function(collectionID, collectionSchema, data) {
        var requireSync;
        for ( var kk in data ) {
            if ( !data.hasOwnProperty(kk) ) {
                continue;
            }

            if ( !collectionSchema[kk] ) {
                continue;
            }

            var columnInfo = collectionSchema[kk].expandable;
            if ( !columnInfo ) {
                continue;
            }

            // its an expandable field
            var dataValue = data[kk];
            var flattened = flat.flatten(dataValue, {'delimiter': '_'});
            for ( var k in flattened ) {
                if ( flattened.hasOwnProperty(k)) {
                    collectionSchema[kk + '_' + k] = { type: "text", required: false };
                }
            }
            // remove original field
            requireSync = true;
        }

        if ( requireSync ) {
            return syncTable( {
                'tableName' : collectionID,
                'attrs' : collectionSchema
            }, false, true);
        } else {
            return q.resolve();
        }
    };

    var postgresObj = {

        /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Public

        /**
         * Save the provided data in a named collection, and return promise
         * @param collectionID
         * @param key
         * @param data
         */
        save : function( collectionID, key, data) {
            collectionID = collectionID.toLowerCase();
            var collectionSchema = schema[collectionID];
            var deferred = q.defer();

            this.init(collectionID).then( function() {

                if ( data && !data['_id'] ) {
                    data._id = key;
                }

                postgresObj.findByID(collectionID, key).then( function(found) {
                    if ( found ) {
                        // destroy existing first
                        return postgresObj.remove(collectionID, key);
                    } else {
                        return q.resolve();
                    }
                }).then( function() {
                    return expandIfNecessary(collectionID, collectionSchema, data);
                }).then( function() {
                    var keys = [];
                    var values = [];
                    var sanitized = {};

                    var dataToSave = {};
                    for (var k in  collectionSchema) {
                        if ( !collectionSchema.hasOwnProperty(k) ) {
                            continue;
                        }
                        var keyParts = k !== '_id' ? k.split('_') : [k];
                        var entry = data;
                        var notFound = false;
                        for ( var kp in keyParts) {
                            if ( entry ) {
                                entry = entry[ keyParts[kp]];
                            } else {
                                notFound = true;
                                break;
                            }
                        }

                        if ( !notFound) {
                            dataToSave[k] = typeof entry === 'object' ? '<__@> ' + JSON.stringify(entry, null, 4) : entry;
                        }
                    }

                    for ( var dataKey in dataToSave ) {

                        if ( dataToSave.hasOwnProperty(dataKey) ) {
                            var value = dataToSave[dataKey];
                            if ( dataKey.indexOf('.') > -1 ) {
                                var originalKey = dataKey;
                                dataKey = sanitize(dataKey);
                                sanitized[dataKey] =  originalKey;
                            }
                            if ( value ) {
                                keys.push( "\"" + dataKey + "\"" );
                                if ( typeof value == 'object' ) {
                                    value = JSON.stringify(value);
                                }
                                values.push(  value ? "'" + value + "'" : 'null' );
                            }
                        }
                    }

                    if ( values.length < 1) {
                        deferred.resolve([]);
                        return;
                    }

                    var sql = "insert into \"" + collectionID + "\" ( " + keys.join(',') + " ) values ( " + values.join(',') + ")";
                    query(sql).then( function(r) {
                        if (r.rowCount < 1 ) {
                            deferred.reject(new Error("failed to insert"));
                            return;
                        }

                        postgresObj.findByID(collectionID, key).then( function(found) {
                            if ( found ) {
                                deferred.resolve(found);
                            } else {
                                deferred.reject(new Error("Could not find what was just inserted"));
                            }
                        });
                    }, function(e) {
                        deferred.reject(e);
                    })

                });
            });

            return deferred.promise;
        },

        /**
         * Retrieve a piece of data from a named collection, based on the criteria, return promise
         * with an array of the results when done.
         * @param collectionID
         * @param criteria
         * @param cursor if true, then returned item is a cursor; otherwise its a concrete collection (array) of items
         */
        find: function( collectionID, criteria, cursor) {
            collectionID = collectionID.toLowerCase();

            var deferred = q.defer();

            this.init(collectionID).then( function() {
                var where = [];

                for ( var dataKey in criteria ) {

                    if ( criteria.hasOwnProperty(dataKey) ) {
                        var original = dataKey;
                        dataKey = sanitize(dataKey);

                        if ( schema[collectionID] && schema[collectionID][dataKey]) {
                            var value = criteria[original];

                            if ( typeof value == 'object') {
                                var $gt = value['$gt'];
                                var $gte = value['$gte'];
                                var $lt = value['$lt'];
                                var $lte = value['$lte'];
                                var $in = value['$in'];

                                dataKey = "\"" + dataKey + "\"";

                                var subClauses = [];
                                if ( $gt ) {
                                    subClauses.push( dataKey + " > '" + $gt + "'");
                                }

                                if ( $gte ) {
                                    subClauses.push( dataKey + " >= '" + $gte  + "'");
                                }

                                if ( $lt ) {
                                    subClauses.push( dataKey + " < '" + $lt + "'" );
                                }

                                if ( $lte ) {
                                    subClauses.push( dataKey + " <= '" + $lte + "'" );
                                }

                                if ( $in ) {
                                    var ins = [];
                                    $in.forEach( function(i) {
                                        ins.push("'" + i + "'");
                                    });
                                    subClauses.push( dataKey + " in (" + ins.join(',') + ")" );

                                }
                                where.push( "(" + subClauses.join(' AND ') + ")");

                            } else {
                                dataKey = "\"" + dataKey + "\"";
                                var whereClause = dataKey + " = '" + value + "'";
                                where.push(whereClause);
                            }
                        } else {
                            deferred.reject(new Error(collectionID + "." + dataKey + " does not exist"));
                            return;
                        }
                    }
                }

                var sql = "select * from \"" + collectionID + "\" ";
                if ( where.length > 0 ) {
                    sql += "where " + where.join(' AND ');
                }
                query(sql).then( function(r) {
                    var results = [];

                    if ( !r || r.rowCount < 1 ) {
                        deferred.resolve(results);
                        return;
                    }

                    // build a json structure from the results, based on '_' delimiter
                    if (r.rows['indexOf']) {
                        r.rows.forEach( function(row) {
                            var obj = hydrate(row);
                            results.push(obj);
                        });
                    } else {
                        results.push(hydrate(r.rows));
                    }


                    if ( !cursor ) {
                        deferred.resolve( results );
                    } else {
                        var stream = ArrayStream(results);
                        // graft next method
                        stream.nextCtr = 0;
                        stream.fullCollection = results;
                        stream.next = function(processorFunction) {
                            if ( !processorFunction ) {
                                return null;
                            }
                            this.nextCtr++;
                            if ( this.nextCtr > this.fullCollection.length - 1 ) {
                                processorFunction(null, null);
                            } else {
                                processorFunction(null, this.fullCollection[this.nextCtr]);
                            }
                        };

                        deferred.resolve(stream );
                    }
                }, function(e) {
                    deferred.reject(e);
                });
            });

            return deferred.promise;
        },

        /**
         * Retrieve a piece of data from a named collection whose key is the one provided.
         * @param collectionID
         * @param key
         */
        findByID: function( collectionID, key ) {
            collectionID = collectionID.toLowerCase();

            var deferred = q.defer();
            postgresObj.init(collectionID).then( function() {
                postgresObj.find( collectionID, {'_id': key}).then( function(r) {
                    if ( r && r.length > 0 ) {
                        deferred.resolve(r[0]);
                    }
                    return deferred.resolve(null);
                }, function(e) {
                    return q.reject(e);
                });
            });

            return deferred.promise;
        },

        /**
         * Remove a piece of data from a name collection, based to the provided key, return promise
         * containing removed items when done.
         * @param collectionID
         * @param key
         */
        remove : function( collectionID, key ) {
            collectionID = collectionID.toLowerCase();

            var deferred = q.defer();

            this.init().then( function() {

                var sql = "delete from \"" + collectionID + "\" where _id = '" + key + "'";
                query(sql).then( function(r) {
                    var results = [];

                    if ( !r || r.rowCount < 1 ) {
                        deferred.reject();
                        return;
                    }

                    deferred.resolve();
                }, function(e) {
                    deferred.reject(e);
                });
            });

            return deferred.promise;
        },

        close: function() {
            return q.resolve();
        },

        destroy: function() {
            var p = q.defer();
            return p.promise;
        },

        /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        query: query,

        init: function(collectionID) {
            collectionID = collectionID ? collectionID.toLowerCase() : undefined;

            var p = q.defer();

            function extracted() {
                if (toSync[collectionID]) {
                    var table = {
                        'tableName': collectionID,
                        'attrs': toSync[collectionID]
                    };
                }
                if (table && !schema[collectionID]) {
                    syncTable(table).then(function () {
                        p.resolve();
                    });
                } else {
                    p.resolve();
                }
            }

            if ( !sync )  {
                var interval = setInterval( function() {
                    if ( sync ) {
                        clearInterval(interval);
                        extracted();
                    }
                }, 1000);
            } else {
                extracted();
            }

            return p.promise;
        },

        sync: function( toSync, dropIfExists ) {
            toSync = toSync || {};
            var p = q.defer();

            this.init().then( function() {

                var proms = [];
                for ( var key in toSync ) {
                    if ( toSync.hasOwnProperty(key) ) {
                        var table = {
                            'tableName' : key,
                            'attrs' : toSync[key]
                        };
                        proms.push( syncTable(table, dropIfExists));
                    }
                }

                q.all(proms).then( function() {
                    p.resolve();
                });

            });

            return p.promise;
        }

    };

    return postgresObj;
};
