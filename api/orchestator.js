var express    = require("express");
//var mysql      = require('mysql');
var bodyParser = require('body-parser');
var uuid       = require('node-uuid');
var amqp       = require('amqp');

var app = express();
app.use(bodyParser.json());

if (typeof String.prototype.endsWith !== 'function') {
    String.prototype.endsWith = function(suffix) {
        return this.indexOf(suffix, this.length - suffix.length) !== -1;
    };
}

var rabbitConn = amqp.createConnection();
var trolley_exchange;

rabbitConn.on('ready', function () {
   console.log("MQ is connected");
   trolley_exchange = rabbitConn.exchange('trolley', {'type': 'topic', 'durable': true});

   // These commands create the queues and bind them to the exchange with those routing keys.
   // But then trigger the 'ready' event again? Because I see it keep logging 'MQ is connected'. Why?
   //rabbitConn.queue('transcodes').bind('trolley', 'jobs::transcode');
   //rabbitConn.queue('shelving').bind('trolley', 'jobs::shelve');
   //rabbitConn.queue('cataloguing').bind('trolley', 'jobs::catalogue');
});


var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database('trolley.db');

db.serialize(function(){
  db.run("CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY, job_id VARCHAR(50), path VARCHAR(256), created_at DATETIME, updated_at DATETIME, status varchar(32), metadata text, title varchar(255), show varchar(100), episode varchar(2), season varchar(2));")
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

  trolley_exchange.publish("jobs::catalogue", job);

  var response = job;
  res.end(JSON.stringify(response));
});

console.log("Set up /cataloguingComplete");
app.post("/cataloguingComplete", function(req,res){
  console.log("Received message to /cataloguingComplete");
  console.log(req.body);

  var job = req.body;
  job.updated_at = new Date;

  db.serialize(function(){
    db.run("UPDATE jobs set updated_at = ?, status='catalogued', path=?, episode=?, season=?, title=?, show=?, metadata=? where job_id=?", [job.updated_at, job.path, job.episode, job.season, job.title, job.show, job.metadata, job.job_id])
  });

  if (job.path.toString().endsWith(".mp4")) {
     trolley_exchange.publish("jobs::shelve", job);
  }
  else {
    trolley_exchange.publish("jobs::transcode", job);
  }

  var response = job;
  res.end(JSON.stringify(response));
});

console.log("Set up /couldNotCatalogue");
app.post("/couldNotCatalogue", function(req,res){
  console.log("Received message to /couldNotCatalogue");
  console.log(req.body);

  var job = req.body;
  job.updated_at = new Date;

  db.serialize(function(){
    db.run("UPDATE jobs set updated_at = ?, status='couldNotCatalogue', path=?, episode=?, season=?, title=?, show=?, metadata=? where job_id=?", [job.updated_at, job.path, job.episode, job.season, job.title, job.show, job.metadata, job.job_id])
  });

  trolley_exchange.publish("jobs::uncatalouged", job);

  var response = job;
  res.end(JSON.stringify(response));
});

console.log("Set up /transcodingComplete");
app.post("/transcodingComplete", function(req,res){
  console.log("Received message to /transcodingComplete");
  console.log(req.body);

  var job = req.body;
  job.updated_at = new Date;

  db.serialize(function(){
    db.run("UPDATE jobs set updated_at = ?, status='transcoded', path=? where job_id=?", [job.updated_at, job.path, job.job_id])
  });

  trolley_exchange.publish("jobs::shelve", job);

  var response = job;
  res.end(JSON.stringify(response));
});

console.log("Set up /shelvingComplete");
app.post("/shelvingComplete", function(req,res){
  console.log("Received message to /shelvingComplete");
  console.log(req.body);

  var job = req.body;
  job.updated_at = new Date;

  db.serialize(function(){
    db.run("UPDATE jobs set updated_at = ?, status='shelved', path=? where job_id=?", [job.updated_at, job.path, job.job_id])
  });

  var response = job;
  res.end(JSON.stringify(response));
});



console.log("Listening on port 3001")
app.listen(3001);
