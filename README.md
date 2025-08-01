# ScyllaDB X Cloud - Tablets and Elasticity

A sample Go application to demonstrate how to watch for traffic and system topology changes and scale accordingly.

## Requirements

- Go 1.19+
- [ScyllaDB X Cloud](https://www.scylladb.com/2025/06/17/xcloud/) cluster
- VPC Peering
- A decent load generator (we used `c6in.8xlarge` and easily hit 400K ops/sec)

## Modes

### Reactive Mode

Monitors cluster capacity and generates traffic that follows a sine wave pattern between baseline and peak capacity. Traffic automatically adjusts when nodes are added or removed from the cluster. The default `1.0` sine amplitude produces a flatline targetting peak capacity.

### API Mode

Performs complete scaling lifecycle testing:

1. Baseline Phase: Sustain baseline traffic
2. Scale-up Phase: Trigger cluster scaling via Cloud API
3. Traffic Ramp: Gradually increase traffic to target peak
4. Peak Phase: Sustain peak traffic
5. Scale-down Phase: Reduce traffic and trigger cluster downscaling
6. Cycle Reset: Return to original cluster size. Rinse and repeat.

## Getting Started

```shell
go mod init xcloud
go get github.com/gocql/gocql
go build -o xcloud xcloud.go
```

## Usage

```shell
MODES
  reactive: Generate traffic pattern based on cluster capacity
  api:      Perform scaling operations using ScyllaDB Cloud API

MANDATORY PARAMETERS:
  -mode string
        Operating mode: 'reactive' or 'api' (default "reactive")
  -node string
        ScyllaDB node address to connect to (default "127.0.0.1")

CLOUD MODE REQUIRED PARAMETERS:
  -token string
        ScyllaDB Cloud API token (can use SCYLLA_CLOUD_TOKEN env var)
  -cluster int
        ScyllaDB Cloud cluster ID
  -target-peak int
        Target peak operations per second for scaling

CLOUD MODE OPTIONAL PARAMETERS:
  -baseline-shard-ops int
        Baseline operations per shard (default 2500)
  -baseline-duration int
        Duration in minutes to sustain baseline traffic before scaling (default 5)
  -peak-duration int
        Duration in minutes to sustain peak traffic before downscaling (default 5)
  -scaling-duration int
        Duration (minutes) it takes for traffic to scale in API mode. (default 10)
        For example:
           1. After 'baseline-duration' elapses, it takes 'scaling-duration' time to reach the current topology peak.
           2. As scaling completes, 'scaling-duration' will take place to reach the defined 'target-peak'
           3. When 'peak-duration' elapses, traffic ramps down within 'scaling-duration'back to its original peak.

REACTIVE MODE PARAMETERS:
  -sine-amplitude float
        1.0 (default) indicates traffic targets the current topology peak (flatline). <=1.0 indicates traffic will fluctuate as a sine
  -sine-duration int
        Half sine cycle in seconds (default 150). How long it takes from sine baseline to peak[1.0] (and vice-versa)

GLOBAL PARAMETERS:
  -keyCount int
        Maximum number of unique keys to generate. Has a direct impact on storage utilization.
  -username string
        ScyllaDB username (default "scylla")
  -password string
        ScyllaDB password (SCYLLA_PASSWORD env, defaults to "scylla")
  -tablets-per-shard int
        Number of tablets per shard (default 64)
  -target-shard-ops int
        Target operations per shard for capacity calculations (default 5000)
  -log-interval int
        Logging interval in seconds (default 15s)
  -keyspace string
        Keyspace name for the test table (default "k")
  -table string
        Table name for the test table (default "t")

ENVIRONMENT VARIABLES:
  SCYLLA_CLOUD_TOKEN    ScyllaDB Cloud API token (alternative to -token)
  SCYLLA_PASSWORD       ScyllaDB password (alternative to -password)

EXAMPLES:
  Reactive mode:
    ./xcloud -mode reactive -node node-0.cluster.scylla.cloud

  API scaling mode:
    ./xcloud -mode api -node node-0.cluster.scylla.cloud -token any -cluster 12345 -target-peak 90000

    export SCYLLA_CLOUD_TOKEN=your-token-here
    ./xcloud -mode api -node node-0.cluster.scylla.cloud -cluster 12345 -target-peak 90000
```

