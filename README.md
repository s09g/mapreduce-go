# mapreduce

MIT 6.824 (2020 Fall) Lab1

Lab code has changed in the new semester. Here is the old version. Don't copy my answer.

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

If you saw the following log when running test，it means you're using a new version of Go instead of 1.13
```bash
rpc.Register: method "Done" has 1 input parameters; needs exactly three
```
