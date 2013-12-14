var testUtils = require('jive-testing-framework/testUtils');
var jive = require('jive-sdk');
var jiveRedis = require('../');

var makeRunner = function() {
    return testUtils.makeRunner( {
        'eventHandlers' : {
            'onTestStart' : function(test) {
                test['ctx']['persistence'] = new jiveRedis({});
            },
            'onTestEnd' : function(test) {
                test['ctx']['persistence'].destroy();
            }
        }
    });
};

makeRunner().runTests(
    {
        'context' : {
            'testUtils' : testUtils,
            'jive' : jive,
            'jiveRedis' : jiveRedis
        },
        'rootSuiteName' : 'jive',
        'runMode' : 'test',
        'testcases' : process.cwd()  + '/library',
        'timeout' : 5000
    }
).then( function(allClear) {
    if ( allClear ) {
        process.exit(0);
    } else {
        process.exit(-1);
    }
});