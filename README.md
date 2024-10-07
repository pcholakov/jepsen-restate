# Jepsen tests for Restate

Build the services distribution first:

```shell
cd services && npm run bundle
```

Start test with:

```shell
lein run test --time-limit 60 --concurrency 6 --rate 6 --workload register --node n1
```
