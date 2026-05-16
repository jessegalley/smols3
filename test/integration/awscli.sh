#!/bin/bash

export AWS_ACCESS_KEY_ID=smols3                                                                                                                              
export AWS_SECRET_ACCESS_KEY=smols3secret                                                                                                                    
export AWS_DEFAULT_REGION=us-east-1                                                                                                                          
alias s3='aws --endpoint-url http://127.0.0.1:9000 s3'                                                                                                       
alias s3api='aws --endpoint-url http://127.0.0.1:9000 s3api'
