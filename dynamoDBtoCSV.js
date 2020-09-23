const program = require("commander");
const AWS = require("aws-sdk");
const unmarshal = require("dynamodb-marshaler").unmarshal;
const Papa = require("papaparse");
const fs = require("fs");

let headers = [];
let unMarshalledArray = [];

program
  .version("0.1.1")
  .option("-t, --table [tablename]", "Add the table you want to output to csv")
  .option("-i, --index [indexname]", "Add the index you want to output to csv")
  .option("-k, --keyExpression [keyExpression]", "The name of the partition key to filter results on")
  .option("-v, --keyExpressionValues [keyExpressionValues]", "The key value expression for keyExpression. See: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html")
  .option("-c, --count", "Only get count, requires -k flag")
  .option("-a, --stats [fieldname]", "Gets the count of all occurances by a specific field name (only string fields are supported")
  .option("-d, --describe", "Describe the table")
  .option("-S, --select [select]", "Select specific fields")
  .option("-r, --region [regionname]")
  .option(
    "-e, --endpoint [url]",
    "Endpoint URL, can be used to dump from local DynamoDB"
  )
  .option("-p, --profile [profile]", "Use profile from your credentials file")
  .option("-m, --mfa [mfacode]", "Add an MFA code to access profiles that require mfa.")
  .option("-f, --file [file]", "Name of the file to be created")
  .option("-l, --lambdaARN [lambdaARN]", "Arn of the lambda to be invoked with every message")
  .option("-lk, --lastKey [lastKey]", "String key of last message from scan, used to start scanning from specific place")
  .option("-batch, --batch [batch]", "Batch max size of records to be sent", 100)
  .option(
    "-ec --envcreds",
    "Load AWS Credentials using AWS Credential Provider Chain"
  )
  .option("-s, --size [size]", "Number of lines to read before writing.", 5000)
  .parse(process.argv);

if (!program.table) {
  console.log("You must specify a table");
  program.outputHelp();
  process.exit(1);
}

if (program.region) {
  AWS.config.update({ region: program.region });
} else {
  AWS.config.loadFromPath(__dirname + "/config.json");
}

if (program.endpoint) {
  AWS.config.update({ endpoint: program.endpoint });
}

if (program.profile) {
  let newCreds = new AWS.SharedIniFileCredentials({ profile: program.profile });
  newCreds.profile = program.profile;
  AWS.config.update({ credentials: newCreds });
}

if (program.envcreds) {
  let newCreds = AWS.config.credentials;
  newCreds.profile = program.profile;
  AWS.config.update({
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
    },
    region: process.env.AWS_DEFAULT_REGION
  });
}

if (program.mfa && program.profile) {
  const creds = new AWS.SharedIniFileCredentials({
    tokenCodeFn: (serial, cb) => {cb(null, program.mfa)},
    profile: program.profile
  });

  // Update config to include MFA
  AWS.config.update({ credentials: creds });
} else if (program.mfa && !program.profile) {
  console.log('error: MFA requires a profile(-p [profile]) to work');
  process.exit(1);
}

const dynamoDB = new AWS.DynamoDB();
const lambda = new AWS.Lambda();

const query = {
  TableName: program.table,
  IndexName: program.index,
  Select: program.count ? "COUNT" : (program.select ? "SPECIFIC_ATTRIBUTES" : (program.index ? "ALL_PROJECTED_ATTRIBUTES" : "ALL_ATTRIBUTES")),
  KeyConditionExpression: program.keyExpression,
  ExpressionAttributeValues: JSON.parse(program.keyExpressionValues),
  ProjectionExpression: program.select,
  Limit: 1000
};

const scanQuery = {
  TableName: program.table,
  IndexName: program.index,
  Limit: 1000
};

// if there is a target file, open a write stream
if (!program.describe && program.file) {
  var stream = fs.createWriteStream(program.file, { flags: 'a' });
}
let rowCount = 0;
let writeCount = 0;
let writeChunk = program.size;

const describeTable = () => {
  dynamoDB.describeTable(
    {
      TableName: program.table
    },
    function (err, data) {
      if (!err) {
        console.dir(data.Table);
      } else console.dir(err);
    }
  );
};

 const scanDynamoDB = (query) => {
  dynamoDB.scan(query, function (err, data) {
    if (!err) {
      unMarshalIntoArray(data.Items); // Print out the subset of results.
      if (data.LastEvaluatedKey) {
        // Result is incomplete; there is more to come.
        query.ExclusiveStartKey = data.LastEvaluatedKey;
        if (rowCount >= writeChunk) {
          // once the designated number of items has been read, write out to stream.
          unparseData(data.LastEvaluatedKey);
        }
        scanDynamoDB(query);
      } else {
        unparseData("File Written");
      }
    } else {
      console.dir(err);
    }
  });
};

const appendStats = (params, items) => {
  for (let i = 0; i < items.length; i++) {
    let item = items[i];
    let key = item[program.stats].S;
  
    if (params.stats[key]) {
      params.stats[key]++;
    } else {
      params.stats[key] = 1;
    }

    rowCount++;
  }
}

const printStats = (stats) => {
  if (stats) {
    console.log("\nSTATS\n----------");
    Object.keys(stats).forEach((key) => {
      console.log(key + " = " + stats[key]);
    });
    writeCount += rowCount;
    rowCount = 0;
  }
}

const processStats = (params, data) => {
  let query = params.query;
  appendStats(params, data.Items);
  if (data.LastEvaluatedKey) {
    // Result is incomplete; there is more to come.
    query.ExclusiveStartKey = data.LastEvaluatedKey;
    if (rowCount >= writeChunk) {
      // once the designated number of items has been read, print the final count.
      printStats(params.stats);
    }
    queryDynamoDB(params);
  } 
};

const processRows = (params, data) => {
  let query = params.query;
  unMarshalIntoArray(data.Items); // Print out the subset of results.
  if (data.LastEvaluatedKey) {
    // Result is incomplete; there is more to come.
    query.ExclusiveStartKey = data.LastEvaluatedKey;
    if (rowCount >= writeChunk) {
      // once the designated number of items has been read, write out to stream.
      unparseData(data.LastEvaluatedKey);
    }
    queryDynamoDB(params);
  } else {
    unparseData("File Written");
  }
};

const queryDynamoDB = (params) => {
  let query = params.query;
  dynamoDB.query(query, function (err, data) {
    if (!err) {
      if (program.stats) {
        processStats(params, data);
      } else {
        processRows(params, data);
      }
    } else {
      console.dir(err);
    }
  });
};

const unparseData = (lastEvaluatedKey) => {
  if (program.file) {
    // writeData(endData);
    console.log('file parameter NOT SUPPORTED ANYMORE');
  } else if (program.lambdaARN) {
    invokeLambda(unMarshalledArray);
  } else {
    console.log(endData);
  }
  // Print last evaluated key so process can be continued after stop.
  console.log("last key:");
  console.log(lastEvaluatedKey);

  // reset write array. saves memory
  unMarshalledArray = [];
  writeCount += rowCount;
  rowCount = 0;
}

const writeData = (data) => {
  stream.write(data);
};

const unMarshalIntoArray = (items) => {
  if (items.length === 0) return;

  items.forEach(function (row) {
    let newRow = {
      eventName: 'MODIFY',
      dynamodb: {
        NewImage: {
          conversationId: {
            S: row['conversationId']['S']
          }
        }
      }
    };
    unMarshalledArray.push(newRow);
    rowCount++;
  });
}

if (program.lastKey) {
  console.log(`Starting from ExclusiveStartKey = '${program.lastKey}'`);
  scanQuery.ExclusiveStartKey = {
    conversationId: { S: program.lastKey }
  }
}

if (program.describe) describeTable(scanQuery);
if (program.keyExpression) queryDynamoDB({ "query": query, stats: {} });
else scanDynamoDB(scanQuery);

/**
 * Lambda invoke part
 */
const invokeLambda = (messages) => {
  console.log(`Invoke lamdba with messages: ${messages.length}`)
  let chunk = Number(program.batch);
  let i,j,temparray;
  for (i=0,j=messages.length; i<j; i+=chunk) {
      temparray = messages.slice(i,i+chunk);
      // do whatever
      console.log(`Sending chunk ${temparray.length}`)
      let params = {
        FunctionName: program.lambdaARN,
        InvocationType: "Event",
        Payload: JSON.stringify({
          Records: temparray
        })
      };
      lambda.invoke(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
      });
  }
}