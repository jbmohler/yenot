# Introduction

The yenot library defines a server framework and JSON data format to talk to a
client library.  It is generally expected that the client & server are both
written in Python.

A plain vanilla Python web framework provides HTTP end-point definitions but
leaves much work to the app developer for authentication and data structure.
Yenot fills in some of these gaps in an a moderately opiniated way but gives
the application developer plug in points to add to the Yenot server.  Yenot
aims to provide:

* JSON data format from server to client
* PostgreSQL integration and server conveniences
* plug in points for additional server end-points
* Rich types through rtlib which know how to format themselves in a gui
* Tabular data conveniences and efficent flexible Python editable tuple type
* Client desktop application in PyQt which can auto-detect lots of things from
  the server and expose a report list and searchable menu structure.

This code originated in a Witmer Public Safety Group internal ERP project but
here is re-incarnated and extended with lessons learned.

# Server Configuration

The following environment variables will configure the server.

* YENOT_HOST -- ip address to all listen on
* YENOT_PORT -- port
* YENOT_DEBUG -- reload, debug or empty

# Test Suite

From a bare linux system it is easiest to run the test suite against a docker
installed postgres.  This very short pointer does not include docker
installation.  Note that after the closing `docker stop` command the postgres
testing instance is completely gone.  Specific db hosting methods are beyond
the scope of this README.

```
docker run --rm --name yenot-test-postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres
sleep 6
docker exec yenot-test-postgres createdb -U postgres -h localhost my_coverage_test
YENOT_DB_URL=postgresql://postgres:mysecretpassword@localhost/my_coverage_test sh full-coverage.sh
docker stop yenot-test-postgres
```
