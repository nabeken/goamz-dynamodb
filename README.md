# GoAMZ-DynamoDB [![Build Status](https://travis-ci.org/nabeken/goamz-dynamodb.png?branch=dev)](https://travis-ci.org/nabeken/goamz-dynamodb)

GoAMZ-DynamoDB is my experimental project forked from [crowdmob/goamz/dynamodb](https://github.com/crowdmob/goamz/tree/master/dynamodb).

These is no gurantees of API stability for now.

## Goals

- cleanup the code base
- cleanup the test code
- keep the code well-tested and reviewed
- keep the code idiomatic go
- add documentation

## API documentation

The API documentation is currently available at:

[http://godoc.org/github.com/nabeken/goamz-dynamodb](http://godoc.org/github.com/nabeken/goamz-dynamodb)

## Running tests

goamz-dynamodb has unittest and integration test using DynamoDB Local and real DynamoDB.

DynamoDB local is managed by [supervisord](http://supervisord.org/).
All you need is to install [virtualenv](http://virtualenv.readthedocs.org/en/latest/).
Our Makefile installs supervisord in virtualenv and starts supervisord.

## DynamoDB local

To download and launch DynamoDB local:

```sh
$ (cd test && make)
```

To test:

```sh
$ go test -v -amazon
```

You can stop supervisord:

```sh
$ (cd test && make stop)
```

## [dynalite](https://github.com/mhart/dynalite)

It is a good alternative to DynamoDB local. Dynalite may allow us to run integrration tests on travis-ci.
I have a plan to add dynalite but contributions are very much welcomed.

### Why do you not run integration tests on travis-ci?

According to LICENSE.txt in DynamoDB local distribution, AWS grants us a license to run DynamoDB local on the machine owned or controlled by us.
we can't use DynamoDB local on travis-ci since travis-ci runs by third party.

## real DynamoDB server on us-east

_WARNING_: Some dangerous operations such as `DeleteTable` will be performed during the tests. Please be careful.

To test:

```sh
$ go test -v -amazon -local=false
```

_Note_: Running tests against real DynamoDB will take several minutes.

## LICENSE

See [LICENSE](LICENSE).
