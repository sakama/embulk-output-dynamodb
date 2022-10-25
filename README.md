# AWS DynamoDB output plugin for Embulk
[![Build Status](https://travis-ci.org/sakama/embulk-output-dynamodb.svg?branch=master)](https://travis-ci.org/sakama/embulk-output-dynamodb)

[Embulk](http://www.embulk.org/) output plugin to load/insert data into [AWS DynamoDB](http://aws.amazon.com/dynamodb/)

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

### General Parameters

- **mode** (string optional, default:upsert)
    - **upsert**
    
        Creates a new item, or replaces an old item with a new item.
        If an item that has the same primary key as the new item already exists in the specified table, the new item completely replaces the existing item.
        This mode uses [BatchWriteItem](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html) and you can set `max_put_items` parameter.
    
    - **upsert_with_expression**
    
        Edits an existing item's attributes, or adds a new item to the table if it does not already exist.
        If you have data with following schema, you can set `update_expression` as follows.
        ```
        - col1(Number primaryKey)
        - col2(Number)
        - col3(String)
        - col4(Number)
        - col5(String)
        ```
        
        ```yaml
        mode: upsert_with_expression
        # update where col1 has #col1
        update_expression: "add #col2 :col2 set #col3 = :col3 set #col4 = #col4 - :col4 remove #col5"
        ```

- **region** (string required) Region of Amazon DynamoDB
- **auto_create_table** (boolean optional, default: false) [See below]()
- **table** (string required) Table name of Amazon DynamoDB

    table and option accept [Time#strftime](http://ruby-doc.org/core-2.2.3/Time.html#method-i-strftime) format to construct table name.
    Table name is formatted at runtime using the local time of the embulk server.
    
    For example, with the configuration below, data is inserted into tables table_2015_04, table_2015_05 and so on.
    ```
    out:
      type: dynamodb
      table: table_%Y_%m
    ```
- **primary_key** (string, required when use auto_create_table) primary key name
- **primary_key_type** (string, required when use auto_create_table) primary key type
- **write_capacity_units** (int optional) Provisioned write capacity units
    - **normal** (int optional) value that will be set after execution
    - **raise** (int optional) value that will be set before execution
- **read_capacity_units** (int optional) Provisioned read capacity units
    - **normal** (int optional) value that will be set after execution
    - **raise** (int optional) value that will be set before execution

        - Up to 30 before execution and down to 5 after execution.
        ```yaml
        write_capacity_units:
        normal: 5
        raise: 30
        ```

        - Up to 30 before execution
        ```yaml
        write_capacity_units:
        raise: 30
        ```
    
        - Down to 5 after execution
        ```yaml
        write_capacity_units:
        normal: 5
        ```

        **NOTICE**: You can decrease the read_capacity_units or write_capacity_units settings for a table, but no more than [4 times per table in a single UTC calendar day](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#d0e53216).

- **max_put_items** (int optional, default:25)
  Max put items at every batch writing requests when `upsert mode`.
  A single call to [BatchWriteItem](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html) can write up to 16 MB of data, which can comprise as many as 25 put requests.
  If you have exceeded data, decrease this value.

### Authentication Parameters

6 authentication methods are supported.

- **basic**

    Use access_key_id and secret_access_key
    
    ```yaml
    auth_method: basic
    access_key_id: YOUR_ACCESS_KEY_ID
    secret_access_key: YOUR_SECRET_ACCESS_KEY
    ```

    - **access_key_id**: AWS access key ID (string, required)
    
    - **secret_access_key**: AWS secret access key (string, required)

- **env**

    Read AWS_ACCESS_KEY_ID (or AWS_ACCESS_KEY) and AWS_SECRET_KEY (or AWS_SECRET_ACCESS_KEY) from environment variables.
    
    ```yaml
    auth_method: env
    ```

- **instance**
    
    Use ECS task role or EC2 instance profile.
    
    ```yaml
    auth_method: instance
    ```

- **profile**

    Read credentials from file. Format of the file is as following, where `[...]` is a name of profile.
    
    ```yaml
    auth_method: profile
    profile_file: /path/to/profilefile
    profile_name: PROFILE_NAME
    ```
    
    - **profile_file**: path to a profiles file. (string, default: given by AWS_CREDENTIAL_PROFILES_FILE environment varialbe, or ~/.aws/credentials).
    
    - **profile_name**: name of a profile. (string, default: `"default"`)
    
    ```
    [default]
    aws_access_key_id=YOUR_ACCESS_KEY_ID
    aws_secret_access_key=YOUR_SECRET_ACCESS_KEY
    
    [profile2]
    ...
    ```

- **properties**

    Read aws.accessKeyId and aws.secretKey from Java system properties
    
    ```yaml
    auth_method: properties
    ```

- **session**

    Use temporary-generated access_key_id, secret_access_key and session_token.
    
    ```yaml
    auth_method: session
    access_key_id: YOUR_ACCESS_KEY_ID
    secret_access_key: YOUR_SECRET_ACCESS_KEY
    session_token: SESSION_TOKEN
    ```
    
    - **access_key_id**: AWS access key ID (string, required)
    
    - **secret_access_key**: AWS secret access key (string, required)
    
    - **session_token**: session token (string, required)

## Example

```yaml
out:
  type: dynamodb
  mode: upsert
  region: us-west-2
  auth_method: basic
  access_key_id: ABCXYZ123ABCXYZ123
  secret_access_key: ABCXYZ123ABCXYZ123
  auto_create_table: true
  table: dynamotable
  primary_key: id
  primary_key_type: Number
  write_capacity_units:
    normal: 5
    raise: 20
  read_capacity_units:
    normal: 6
    raise: 30
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

## Test

```
$ ./gradlew test  # -t to watch change of files and rebuild continuously
```

To run unit tests, you need to setup DynamoDB on your local environment.
Please reference the documentation [Running DynamoDB on Your Computer](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html) on AWS Developer Guide.
```shell-session
$ mkdir /path/to/dynamodb
$ cd /path/to/dynamodb
$ wget http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest.tar.gz
$ tar zxvf dynamodb_local_latest.tar.gz
$ java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb
```
