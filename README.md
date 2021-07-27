# mapreduce

```bash
$ ./test-mr.sh        
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS
```

如果你看见下面的log，说明你用了新的Go版本，而不是1.13
```bash
rpc.Register: method "Done" has 1 input parameters; needs exactly three
```
