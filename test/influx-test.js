var assert = require('assert')
var seneca = require('seneca')
var async = require('async')
var shared = require('seneca-store-test')
var seneca = require('seneca');

var username = 'root';
var password = 'root';
var database = 'test_db';
var host = 'localhost';

var spec = {host : host, username : username, password : password, database : database};

var si = seneca()
si.use(require('..'), spec);

si.__testcount = 0
var testcount = 0


describe('influx', function(){
  it('basic', function(done){
    testcount++
    shared.basictest(si,done)
  })

  /*it('extra', function(done){
    testcount++
    extratest(si,done)
  })

  it('close', function(done){
    shared.closetest(si,testcount,done)
  })*/
})