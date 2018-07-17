# clj-3df

**This is currently a research project, not feature-complete in any
way, and overall not ready for use in production services.**

3DF is best thought of as a pub/sub system in which subscriptions can
be arbitrary Datalog expressions. Subscribers register queries with
the broker, and data sources (such as Kafka, Datomic, or any other
source-of-truth) publish new data to it. All subscriber queries
affected by incoming data will be notified with a diff, describing how
their results have changed. The Datalog implementation is modeled
after [Datomic's query
language](https://docs.datomic.com/on-prem/query.html) and aims to
support the same set of features.

3DF does this efficiently, thanks to being built on top of
[differential
dataflows](https://github.com/frankmcsherry/differential-dataflow). In
particular, for any given transaction asserting / retracting n facts,
3DF will at worst do work in `O(n * # of registered queries)`. Often
it can be even less than that, because dataflows can be shared between
similar queries.

This repository contains the Clojure client for 3DF. The broker is
written in Rust and can be found in the [Declarative Differential
Dataflows](https://github.com/comnik/declarative-dataflow) repository.

## How it works

The 3DF client compiles Datalog expressions into an intermediate
representation that can be synthesised into a differential dataflow on
the server. This dataflow is then registered and executed across any
number of workers. Whenever query results change due to new data
entering the system, the server will push the neccessary changes via a
WebSocket connection.

For example, consider a subscriber created the following subscription:

``` clojure
(exec! conn
  (register-query db "user inbox" 
    '[:find ?msg ?content
      :where 
	  [?msg :msg/recipient "me@nikolasgoebel.com"]
	  [?msg :msg/content ?content]]))
```

and a new message arrives in the system.

``` clojure
[{:msg/receipient "me@nikolasgoebel.com"
  :msg/content    "Hello!"}]
```

Then the server will push the following results to the subscriber:

``` clojure
[[[<msg-id> "Hello!"] +1]]
```

If at some later point in time, this message was retracted

``` clojure
[[:db/retractEntity <msg-id>]]
```

the server would again notify the subscriber, this time indicating the
retraction:

``` clojure
[[[<msg-id> "Hello!"] -1]]
```

This guarantees, that subscribers maintaining any form of functionally
derived information will always have a consistent view of the data.

## Query Language Features

- [x] Implicit joins and unions, `and` / `or` operators
- [x] Stratified negation
- [ ] Parameterized queries
- [x] Rules, self-referential / mutually recursive rules
- [x] Basic aggregates (min, max, count)
- [ ] Grouping via `:with`
- [x] Basic predicates (<=, <, >, >=, =, not=)
- [ ] As-of queries
- [ ] More find specifications (e.g. collection, scalar)
- [ ] Pull queries

## Non-Features

3DF is neither concerned with durability nor with consistency in the
ACID sense. It is intended to be used in combination with a
consistent, durable source-of-truth such as
[Datomic](https://www.datomic.com/) or
[Kafka](https://kafka.apache.org/).

Consequently, 3DF will accept whatever tuples it is supplied with. For
example, whereas in Datomic two subsequent transactions on an empty
database

``` clojure
(d/transact conn [[:db/add 123 :user/balance 1000]])
...
(d/transact conn [[:db/add 123 :user/balance 1500]])
```

would result in the following sets of datoms being added into the
database:

``` clojure
[[123 :user/balance 1000 <tx1> true]]
...
[[123 :user/balance 1000 <tx2> false]
 [123 :user/balance 1500 <tx2> true]]
```

3DF will by itself not take any previous information into account on
transactions. Again, 3DF is intended to be fed data from a system like
Datomic, which would ensure that transactions produce consistent
tuples.

## License

Copyright © 2018 Nikolas Göbel

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
