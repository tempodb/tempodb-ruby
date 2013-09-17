## 0.6.1

* Fixes mutation of inbound filter params

## 0.6.0

* Add delete series
* Add multi write and increment

## 0.5.7

* Use source encoding in Regexp (instead of "none")

## 0.5.6

* Bugfix - Support forward slashes in series keys

## 0.5.5

* Bugfix - URI encode our paths (not just params)

## 0.5.4

* Switch to HTTPClient, for thread-safe persistent HTTP connections

## 0.5.3

* Bugfix - Fix support for ruby 1.8.7

## 0.5.2

* Bugfix - when wanting SSL and persistent connections, start the connection with SSL - **Thanks [ejfinneran](https://github.com/ejfinneran)**
