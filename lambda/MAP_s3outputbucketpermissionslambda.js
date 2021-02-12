          const AWS = require('aws-sdk');
          const util = require('util');
          exports.handler = function(event, context, callback) {
              
              // If its an object delete, do nothing
              if (event.RequestType === 'Delete') {
              }
              else // Its an object put
              {
                  // Get the source bucket from the S3 event
                  var srcBucket = event.Records[0].s3.bucket.name;      
                  // Object key may have spaces or unicode non-ASCII characters, decode it
                  var srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));  
                  // Define the object permissions, using the permissions array
                  var params =
                  {
                      Bucket: srcBucket,
                      Key: srcKey,
                      AccessControlPolicy:
                      {
                          'Owner':
                          {
                              'DisplayName': process.env.payeraccountid.toString(),
                              'ID': process.env.canonicalidpayer.toString()
                          },
                          'Grants': 
                          [
                              {
                                  'Grantee': 
                                  {
                                      'Type': 'CanonicalUser',
                                      'DisplayName': process.env.payeraccountid.toString(),
                                      'ID': process.env.canonicalidpayer.toString()
                                  },
                                  'Permission': 'FULL_CONTROL'
                              },
                              {
                                  'Grantee': {
                                      'Type': 'CanonicalUser',
                                      'DisplayName': process.env.linkedaccountid.toString(),
                                      'ID': process.env.canonicalidlinked.toString()
                                      },
                                  'Permission': 'READ'
                              },
                          ]
                      }
                  };

                  // get reference to S3 client 
                  var s3 = new AWS.S3();
                  // Put the ACL on the object
                  s3.putObjectAcl(params, function(err, data) {
                      if (err) console.log(err, err.stack); // an error occurred
                      else     console.log(data);           // successful response
                  });
              }
           };
