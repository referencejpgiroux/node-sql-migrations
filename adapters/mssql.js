var mssql = require('mssql');

module.exports = function (config, logger) {
    var pool = new mssql.ConnectionPool({
        server: config.host,
        port: config.port,
        user: config.user,
        password: config.password,
        driver: "SQL Server",
    });

    var databse = config.db;

    function ensureConnected() {
       var promise = new Promise(
          function(resolve, reject) {
            if(!pool.connected) {
                pool.connect(function(err){
                    if(err){reject(err);return;}
                    
                    return pool.query('IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = \''+ databse +'\')\n' +
                        'CREATE DATABASE [' + databse + ']')
                        .then(function() {
                            pool.query('USE ' + databse).then(function(){
                                resolve();
                            });
                        });
                });
            } else {
                resolve();
            }
          } 
       );
       return promise;
    }


    function exec(query, values) {
        return ensureConnected().then(function() {
            return pool.query(query, values)
                .then(function (result) {
                    if(result != undefined && result.recordsets != undefined) {
                        return result.recordsets[0];
                    } else {
                        return result;
                    }
                }
            );    
        });
    }

    function ensureMigrationTableExists() {
        return exec('SELECT * FROM sysobjects WHERE name=\'__migrations__\' and xtype=\'U\'')
            .then(function(result){
                if(result[0] == undefined || result[0].name != '__migrations__') {
                    return exec('CREATE TABLE __migrations__ (\n' +
                        'id bigint not null)'
                    );
                }
            });
    }

    return {
        appliedMigrations: function appliedMigrations() {
            return ensureMigrationTableExists().then(function () {
                return exec('select * from __migrations__');
            }).then(function (result) {
                return result.map(function (row) { return row.id; });
            });
        },
        applyMigration: function applyMigration(migration, sql) {
            return exec(sql).then(function (result) {
                logger.log('Applying ' + migration);
                logger.log(result);
                logger.log('===============================================');
                var values = [migration.match(/^(\d)+/)[0]];
                return exec(`insert into __migrations__ (id) values (${values})`);
            });
        },
        rollbackMigration: function rollbackMigration(migration, sql) {
            return exec(sql).then(function (result) {
                logger.log('Reverting ' + migration);
                logger.log(result);
                logger.log('===============================================');
                var values = [migration.match(/^(\d)+/)[0]];
                return exec(`delete from __migrations__ where id = ${values}`);
            });
        },
        dispose: function dispose() {
            return pool.close();
        }
    };
};
