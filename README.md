# kvproxy

Act as a proxy for a memcached cluster.


# Running

```go run .```

# Testing

```bash
memcached -p 11213 &
go test -coverpkg=all ./...
```