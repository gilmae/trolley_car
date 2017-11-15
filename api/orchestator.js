var express    = require("express");
//var mysql      = require('mysql');
var bodyParser = require('body-parser');
var uuid       = require('node-uuid');

var AWS = require('aws-sdk');
var AWS = require('aws-sdk/global');

const catalogQueueName = 'cataloguing';
const transcodeQueueName = 'transcodes';
const shelvingQueueName = 'shelving';

var sqs = new AWS.SQS({apiVersion: '2012-11-05', endpoint: 'http://localhost:9324', region:"ap-southeast-2", accessKeyId:"invalid", secretAccessKey:'invalid'});
sqs.createQueue({QueueName: catalogQueueName}, function(err, data){if (err){console.log(err)}});
sqs.createQueue({QueueName: transcodeQueueName}, function(err, data){if (err){console.log(err)}});
sqs.createQueue({QueueName: shelvingQueueName}, function(err, data){if (err){console.log(err)}});

function getQueueUrl(queueName){
  return new Promise((resolve, reject) => {
    sqs.getQueueUrl({QueueName: queueName}, function(err, data) {
      if (err)
      {
        reject(err);
      }
      else
      {
        resolve(data);
      }
    })
  });
}

function sendToQueue(queueName, job)
{
  return new Promise((resolve, reject) => {
    getQueueUrl(catalogQueueName).then((data) => {
      let queueName = data.QueueUrl;
      sqs.sendMessage(
        {
          QueueUrl: queueName, 
          MessageBody: JSON.stringify(job)
        }, 
        function(err,d)
        {
          if (err) {
            reject(err);
          }
          else
          {
            resolve(d);
          }
        }
      );
    })
    .catch((err)=>{
     resolve(err);
    });
  });
}

var app = express();
app.use(bodyParser.json());

if (typeof String.prototype.endsWith !== 'function') {
    String.prototype.endsWith = function(suffix) {
        return this.indexOf(suffix, this.length - suffix.length) !== -1;
    };
}

console.log("API Starting");
console.log("Set up /registerJob");

app.post("/registerJob", function(req,res){
  console.log("Received message to /registerJob");
  console.log(req.body);

  var job = req.body;
  job.job_id = uuid.v4();
  job.created_at = new Date;
  job.updated_at = job.created_at;

  sendToQueue(catalogQueueName, job).then((data)=> 
  {
    var response = job;
    res.end(JSON.stringify(response));
  })
  .catch((err) => 
  {
    console.log(err);
    res.end();
  });
});

console.log("Set up /cataloguingComplete");
app.post("/cataloguingComplete", function(req,res){
  console.log("Received message to /cataloguingComplete");
  console.log(req.body);

  var job = req.body;
  job.updated_at = new Date;

  let queueName = transcodeQueueName;

  if (job.path.toString().endsWith(".mp4")) {
    queueName = shelvingQueueName;
  }

  sendToQueue(queueName, job).then((data)=> 
  {
    var response = job;
    res.end(JSON.stringify(response));
  })
  .catch((err) => 
  {
    console.log(err);
    res.end();
  });
});

// console.log("Set up /couldNotCatalogue");
// app.post("/couldNotCatalogue", function(req,res){
//   console.log("Received message to /couldNotCatalogue");
//   console.log(req.body);

//   var job = req.body;
//   job.updated_at = new Date;

//   //db.serialize(function(){
//   //  db.run("UPDATE jobs set updated_at = ?, status='couldNotCatalogue', path=?, episode=?, season=?, title=?, show=?, metadata=?, type=? where job_id=?", [job.updated_at, job.path, job.episode, job.season, job.title, job.show, job.metadata, job.type, job.job_id])
//   //});

//   trolley_exchange.publish("jobs::uncatalouged", job);

//   var response = job;
//   res.end(JSON.stringify(response));
// });

console.log("Set up /transcodingComplete");
app.post("/transcodingComplete", function(req,res){
  console.log("Received message to /transcodingComplete");
  console.log(req.body);

  var job = req.body;
  job.updated_at = new Date;

  sendToQueue(shelvingQueueName, job).then((data)=> 
  {
    var response = job;
    res.end(JSON.stringify(response));
  })
  .catch((err) => 
  {
    console.log(err);
    res.end();
  });
});

console.log("Set up /shelvingComplete");
app.post("/shelvingComplete", function(req,res){
  console.log("Received message to /shelvingComplete");
  console.log(req.body);

  var job = req.body;
  job.updated_at = new Date;

  //sendToQueue(queueName, job).then((data)=> 
  // {
  //   var response = job;
  //   res.end(JSON.stringify(response));
  // })
  // .catch((err) => 
  // {
  //   console.log(err);
  //   res.end();
  // });

  var response = job;
  res.end(JSON.stringify(response));
});

 console.log("Listening on port 3001")
 app.listen(3001);
