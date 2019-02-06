# 3DF Examples

Running examples requires a recent Clojure environment, procurement of
which is described here:
https://clojure.org/guides/getting_started. 

Make sure to start a [3DF
server](https://github.com/comnik/declarative-dataflow) locally on
port `6262`. Once that is available,from within the repository root,
run:

``` shell
clj -m <example_name>
```

In most cases you would rather want to open the example source and a
repl in your favourite Cloujre development environment and play around
with the examples interactively.

There is also a runner provided, that can run experiments defined as
data (the RGA example is provided as data at
`examples/queries/rga.edn`):

``` shell
clj -m runner <filename>
```
