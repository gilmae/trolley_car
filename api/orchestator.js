var express    = require("express");
//var mysql      = require('mysql');
var bodyParser = require('body-parser');
var uuid       = require('node-uuid');
var amqp       = require('amqp');

var app = express();
app.use(bodyParser.json())

var rabbitConn = amqp.createConnection();
var trolley_exchange;

rabbitConn.on('ready', function () {
   console.log("MQ is connected");
   trolley_exchange = rabbitConn.exchange('trolley', {'type': 'topic'});
});


var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database('trolley.db');

db.serialize(function(){
  db.run("CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY, job_id VARCHAR(50), path VARCHAR(256), created_at DATETIME, updated_at DATETIME, status varchar(32), metadata text);")
});

console.log("API Starting");
console.log("Set up /registerJob");

app.post("/registerJob",function(req,res){
  console.log("Received message to /registerJob");
  console.log(req.body);

  var job = req.body;
  job.job_id = uuid.v4();
  job.created_at = new Date;
  job.updated_at = job.created_at;

  db.serialize(function(){
    db.run("INSERT INTO jobs (job_id, created_at, updated_at, path, status) VALUES (?,?,?,?, 'new')", [job.job_id, job.created_at, job.updated_at, job.path]);
  })

  trolley_exchange.publish("jobs::transcode", job);

  var response = job;
  res.end(JSON.stringify(response));
});

app.post("/transcodingComplete", function(req,res){
  console.log("Received message to /transcodingComplete");
  console.log(req.body);

  var job = req.body;
  job.updated_at = new Date;

  db.serialize(function(){
    db.run("UPDATE jobs set updated_at = ?, status='transcoded', path=? where job_id=? and status='new'", [job.updated_at, job.path, job.job_id])
  });

  trolley_exchange.publish("jobs::catalogue", job);

  var response = job;
  res.end(JSON.stringify(response));
});

app.post("/cataloguingComplete", function(req,res){
  console.log("Received message to /cataloguingComplete");
  console.log(req.body);

  var job = req.body;
  job.updated_at = new Date;

  db.serialize(function(){
    db.run("UPDATE jobs set updated_at = ?, status='catalogued', path=? where job_id=? and status='transcoded'", [job.updated_at, job.path, job.job_id])
  });
  var response = job;
  res.end(JSON.stringify(response));
});

console.log("Listening on port 3001")
app.listen(3001);
