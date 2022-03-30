# tdkvs

[![Tests](https://github.com/orellazri/tdkvs/actions/workflows/tests.yml/badge.svg)](https://github.com/orellazri/tdkvs/actions/workflows/tests.yml)

Distributed key-value store in Go with support for rebalancing.

The store consists of a master server and `n` volume servers. The master server uses BadgerDB to store the "metakeys" - the keys that are saved in the store, and which volume server they are stored in. The volume servers store the data as files.

The store uses [jump consistent hash](https://arxiv.org/pdf/1406.2294.pdf) to quickly and efficiently calculate the correct bucket (volume server) in the range [0, n) to store the key.

## Usage

Download the source code and build.

### Master server

```bash
./master -config=<config file>
```

The config yaml file for the master server should be as follows:

```yaml
port: 3000
volumes:
  - http://10.0.0.1:3001
  - http://10.0.0.2:3001
  - http://10.0.0.3:3001
```

### Volume server

```bash
./volume -config=<config file>
```

The config yaml file for the volume server should be as follows:

```yaml
port: 3001
path: /storage_directory/
```

## Automatic Rebalancing

When the master server is started, it checks if volume servers have been added. If so, it rebalances some keys by moving them to other volume servers in order to get a balanced distribution using jump consistent hash.

So, if you wish to add a volume server, you need to change the master's config yaml file accordingly, make sure all the volume servers are running, and restart the master server.
