# GoAMZ-DynamoDB [![Build Status](https://travis-ci.org/nabeken/goamz-dynamodb.png?branch=dev)](https://travis-ci.org/nabeken/goamz-dynamodb)

GoAMZ-DynamoDB is my experimental project forked from [crowdmob/goamz/dynamodb](https://github.com/crowdmob/goamz/tree/master/dynamodb).

These is no gurantees of API stability for now.

## API version

- [v1](http://gopkg.in/nabeken/goamz-dynamodb.v1) has the compatibility with `crowdmob/goamz/dynamodb`.
- [v2](http://gopkg.in/nabeken/goamz-dynamodb.v2) will have breaking changes.

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

Thanks to dynalite, goamz-dynamodb is now integrated on every commit on Travis CI.

goamz-dynamodb has unittest and integration test using DynamoDB Local, dynalite and real DynamoDB.

DynamoDB local and dynalite are managed by [supervisord](http://supervisord.org/).
You need to install [virtualenv](http://virtualenv.readthedocs.org/en/latest/) and nodejs if you want to run the tests against dynalite.
Our Makefile installs supervisord automatically in virtualenv.

### supervisord

```sh
$ (cd test && make supervisord)
```

You can stop supervisord:

```sh
$ (cd test && make stop)
```
### DynamoDB local

To download and launch DynamoDB local:

```sh
$ ./test/setup-dynamodblocal.sh
$ (cd test && ./venv/bin/supervisorctl start dynamodb_local)
```

To test:

```sh
$ go test -v -integration -provider=local
```

### Why do you not run integration tests on travis-ci?

According to LICENSE.txt in DynamoDB local distribution, AWS grants us a license to run DynamoDB local on the machine owned or controlled by us.
we can't use DynamoDB local on travis-ci since travis-ci runs by third party.

### [dynalite](https://github.com/mhart/dynalite)

You need to install nodejs before installing dynalite.

```sh
$ npm install -g dynalite
$ (cd test && ./venv/bin/supervisorctl start dynalite)
```

To test:

```sh
$ go test -v -integration -provider=dynalite
```

### real DynamoDB server on us-east

_WARNING_: Some dangerous operations such as `DeleteTable` will be performed during the tests. Please be careful.

To test:

```sh
$ go test -v -integration -provider=amazon
```

_Note_: Running tests against real DynamoDB will take several minutes.

## LICENSE

See [LICENSE](LICENSE).
