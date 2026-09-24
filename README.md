# goguard

A MySQL-aware TCP proxy. It sits between clients and a MySQL server, frames
the wire protocol packet by packet, and logs the commands clients send.

## Run

```sh
go run ./cmd/goguard -listen :8080 -upstream 127.0.0.1:3306
mysql -h 127.0.0.1 -P 8080 -u root -p   # connect through the proxy
```

| Flag           | Default          | Meaning                                                 |
|----------------|------------------|---------------------------------------------------------|
| `-listen`      | `:8080`          | Address clients connect to                              |
| `-upstream`    | `127.0.0.1:3306` | MySQL server to forward to                              |
| `-log-queries` | `true`           | Log each client command, with SQL for queries/prepares  |
| `-log-packets` | `false`          | Log direction, sequence id and length of every packet   |

Query logs contain raw SQL, which can include sensitive values (for example
`CREATE USER ... IDENTIFIED BY '...'`). Use `-log-queries=false` where that
matters.

## Layout

```
cmd/goguard/      entry point: flags, listener, signal handling
internal/mysql/   MySQL protocol pieces: packet framing, command parsing
internal/proxy/   accepting connections and relaying packets both ways
```

`internal/mysql` knows nothing about networking, and `internal/proxy` knows
nothing about protocol details beyond calling into `mysql`. New protocol
awareness (e.g. parsing server responses) belongs in `mysql`; new proxy
behaviour (e.g. blocking a query) hooks into the per-packet `inspector` in
`proxy`.

## Test

```sh
go test -race ./...
```
