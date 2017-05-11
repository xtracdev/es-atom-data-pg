# Event Source Atom Data: Postgres Implementation 

This project processes events from the Postgres Event Store published via
[pgpublish](https://github.com/xtracdev/pgpublish) to SNS and 
consumed by this project via an SQS queue subscribed to the topic, 
writing the events 
into organized storage to allow navigating published events using 
the atom protocol.

This project uses two tables: an event table to store the events associated 
with the atom feed, and the feed table, which keeps track of the feeds. 
The overall event history can be traversed from the current feed 
backwards using the previous entry in the feeds table. The recent items 
are those that have not been assigned a feedid.

As events get written to the recent table, once the size threshold for 
a feed is read, they are assigned a feed id. The default page size is 
100 items; this may be overridden using the FEED_THRESHOLD 
environment variable.

## Contributing

## Contributing

To contribute, you must certify you agree with the [Developer Certificate of Origin](http://developercertificate.org/)
by signing your commits via `git -s`. To create a signature, configure your user name and email address in git.
Sign with your real name, do not use pseudonyms or submit anonymous commits.


In terms of workflow:

0. For significant changes or improvement, create an issue before commencing work.
1. Fork the respository, and create a branch for your edits.
2. Add tests that cover your changes, unit tests for smaller changes, acceptance test
for more significant functionality.
3. Run gofmt on each file you change before committing your changes.
4. Run golint on each file you change before committing your changes.
5. Make sure all the tests pass before committing your changes.
6. Commit your changes and issue a pull request.

## License

(c) 2016 Fidelity Investments
Licensed under the Apache License, Version 2.0
