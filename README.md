# tdkvs

[![Tests](https://github.com/orellazri/tdkvs/actions/workflows/tests.yml/badge.svg)](https://github.com/orellazri/tdkvs/actions/workflows/tests.yml)

Distributed key-value store in Go with support for rebalancing and deleting volume servers.

The store consists of a master server and _n_ volume servers. The master server uses BadgerDB to store the "metakeys" - the keys that are saved in the volume servers, and which volume server they are stored in. The volume servers store the data as files.

The store uses [jump consistent hash](https://arxiv.org/pdf/1406.2294.pdf) to quickly and efficiently calculate the correct bucket (volume server) in the range _[0, n)_ to store the key.

## Automatic Rebalancing

When the master server is started, it checks if volume servers have been added. If so, it rebalances some keys by moving them to other volume servers in order to get a balanced distribution using jump consistent hash.

So, if you wish to add a volume server, you need to change the master's config yaml file accordingly, make sure all the volume servers are running, and restart the master server.

**NOTE:** When adding volume servers, make sure to add them to the bottom of the list in the master's config yaml file since the store works with ascending indices.

## Volume Server Deletion

When deleting a volume server, the store will move all its keys to a new volume server chosen by the jump consistent hash alogirthm. It will then re-balance the cluster.

In order to delete a volume server from the cluster, you need to follow these steps:

- Make sure the master's config yaml file contains all the volume servers including the one you wish to delete
- Make sure all the volume servers are up, including the one you wish to delete
- Shut down the master server and run with `./tdkvs master -config=<config file> -delete=<index>` where `index` is the index of the volume server in the yaml file (starting from 0)
- Remove the volume server from the master's config yaml file and shut the volume server down
- Re run the master server as usual

## Usage

Download the source code and build.

### Master server

```bash
./tdkvs master -config=<config file>
```

The config yaml file for the master server should be as follows:

```yaml
port: 3000
volumes:
  - http://10.0.0.1:3001
  - http://10.0.0.2:3001
  - http://10.0.0.3:3001
```

### Volume servers

```bash
./tdkvs volume -config=<config file>
```

The config yaml file for the volume server should be as follows:

```yaml
port: 3001
path: /storage_directory/
```
