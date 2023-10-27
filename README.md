# rbe-grpc-debug-proxy

reverse gRPC proxy that implements bazel remote APIs. prints out all grpc calls to the upstream server.

## usage

```
$ make
$ ./rbe-debug-proxy
Usage of ./rbe-grpc-debug-proxy:
  -ca_cert string
        ca file
  -client_cert string
        client cert file
  -client_cert_key string
        client cert key file
  -port string
        port (optional) (default "8080")
  -target string
        proxy target
  -unix string
        unix socket (optional)
```

## example

```
# ./rbe-debug-proxy [args]
checking connectivity to some.domain... ok
proxy server listening on grpc://localhost:8080
2023/10/27 18:06:54 Called GetCapabilities
2023/10/27 18:06:54 Called GetActionResult
2023/10/27 18:06:54 Called FindMissingBlobs
2023/10/27 18:06:55 Called Write
2023/10/27 18:06:55 Called Write
2023/10/27 18:06:55 Called Write
2023/10/27 18:06:55 Called Write
2023/10/27 18:06:55 Called Write
2023/10/27 18:06:55 Called Write
2023/10/27 18:06:56 Called Execute: hash:"28e138badff64cba7cc90633fc7248b560ff7cf116bbbede034d207a214500bf"  size_bytes:510
2023/10/27 18:06:57 Called Read: blobs/511681c9dfd04a4079be64e995b7be34a7393d5d77afe072c002f001fff7118e/68
2023/10/27 18:06:57 Called Read: blobs/dd2215ba3757abcfa884dbbe9c1ccb27daf160c9c0c59ea7ea4cd45eb3a1319e/79
2023/10/27 18:06:57 Called Read: blobs/6c2ca51e45b2e3583f1baf3cbf1b77faec79801e55135c1ae9ef1ce885dd6fee/599
2023/10/27 18:06:57 Called Read: blobs/8e609bb71c20b858c77f0e9f90bb1319db8477b13f9f965f1a1e18524bf50881/11
2023/10/27 18:06:57 Called Read: blobs/f769dcc6ba1f9b97056412a7d3f7991f5e9a3edc15713eb333234c715b9ea1a1/100
2023/10/27 18:06:57 Called Read: blobs/b329b5e9bda351e66df229d74557439d8b707bd7e8247987c3e1fb8bec93c45c/919
2023/10/27 18:06:57 Called Read: blobs/9c462001c638df7b5cc5334a2796eb235d25af2006f052a5b74f5de272d3c40a/84
2023/10/27 18:06:57 Called Read: blobs/26a182cdb0450cf50ab5fc8135a9fce08794d0a83fb0c8d3661dfb7561f90deb/572
2023/10/27 18:06:57 Called Read: blobs/ebf1227276d8553a33bd760ef8dd9d368b72d6430681cb83b230f48c8819fa11/326
2023/10/27 18:06:57 Called Read: blobs/bc762654be336a14d9ed89ea36aeee4ab8b15f6d55931790178cc4dd53eca483/66
2023/10/27 18:06:57 Called Read: blobs/5be61c46ec7ea38c73f49a89aa2cbc2635bc292a4078df48991da2e8d1ebb015/63
2023/10/27 18:06:57 Called Read: blobs/5100c26e5b8b6698b7225d090f3ed621748e4273932b45968b37078e3f352e89/205
2023/10/27 18:06:57 Called Read: blobs/443831ea2b05d9cc9212b1f5aaaeb07a38d59fff61e22750eb6d23fdddfeeb17/764
2023/10/27 18:06:57 Called Read: blobs/4314f97f0a78b36cafc387f16078e69b41c8727404f057ab4533bfbe2d5ccb17/231
2023/10/27 18:06:57 Called Read: blobs/a935106bd6424dfde76b2a9c8655c7c00abaa26e298928d47ea1a47315fcc655/112
2023/10/27 18:06:57 Called Read: blobs/496bcba4752268c612212928d90b192b4da0ec7ff0b8b4b9dc37cefd62f0e130/150
2023/10/27 18:06:57 Called Read: blobs/e32a9bbf8724349ed5f18b715172a062e84d3b354f0e66820f8e7744ac008a20/144
2023/10/27 18:06:57 Called Read: blobs/5fc920a62cddc551035376cf5da461f12998faeee8776cc1e871469409595a3c/838
```
