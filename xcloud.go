package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

type Config struct {
	Mode, Node, Token, Username, Password, KeyspaceName, TableName string
	ClusterID, TargetPeakOps, BaselineOpsPerShard, BaselineDuration, PeakDuration, ScalingDuration int
	SineDuration, TabletsPerShard, TargetShardOps, LogInterval, KeyCount int
	SineAmplitude float64
}

type ScalingPhase int
const (Baseline ScalingPhase = iota; PreScale; Scaling; PostScale; Peak; ScaleDown; Decommission)
func (p ScalingPhase) String() string {
	return []string{"Baseline", "PreScale", "Scaling", "PostScale", "Peak", "ScaleDown", "Decommission"}[p]
}

type ClusterState struct{ Nodes, Shards, MinRackShards, DecommissioningNodes int; Balanced bool }

type App struct {
	config Config
	session *gocql.Session
	apiClient *http.Client
	tableId gocql.UUID
	
	// Traffic state
	currentBaseline, currentPeak, newBaseline, newCapacity int
	timeOffset float64
	transitionPending bool
	lastShardCount int
	successCount, failureCount, keyCounter uint64
	valPool [][]byte
	
	// Scaling state
	scalingPhase ScalingPhase
	phaseStartTime time.Time
	originalNodes, originalShards, scaleNodes, postScaleStartTraffic int
	scaleUpTriggered, scaleDownTriggered bool
	accountId, dcId, instanceTypeId int
}

// Suppress gocql warnings
type quietLogger struct{}
func (quietLogger) Printf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	if !strings.Contains(msg, "Found invalid peer") && !strings.Contains(msg, "buffer full") {
		log.Print(msg)
	}
}
func (quietLogger) Print(v ...interface{}) {}
func (quietLogger) Println(v ...interface{}) {}

func parseFlags() Config {
	var c Config
	flag.StringVar(&c.Mode, "mode", "reactive", "Mode: 'reactive' or 'api'")
	flag.StringVar(&c.Node, "node", "127.0.0.1", "ScyllaDB node (127.0.0.1)")
	flag.StringVar(&c.Token, "token", "", "Cloud API token (env SCYLLA_CLOUD_TOKEN)")
	flag.IntVar(&c.KeyCount, "keyCount", 200000000, "Maximum number of unique keys to generate (200_000_000)")
	flag.IntVar(&c.ClusterID, "cluster", 0, "Cluster ID (mandatory for api mode)")
	flag.IntVar(&c.TargetPeakOps, "target-peak", 0, "Target peak ops/sec (mandatory for api mode)")
	flag.IntVar(&c.BaselineOpsPerShard, "baseline-shard-ops", 2500, "Baseline ops per shard (2500)")
	flag.IntVar(&c.BaselineDuration, "baseline-duration", 5, "Baseline duration (minutes)")
	flag.IntVar(&c.PeakDuration, "peak-duration", 5, "Peak duration (minutes)")
	flag.IntVar(&c.ScalingDuration, "scaling-duration", 10, "Scaling duration (minutes)")
	flag.Float64Var(&c.SineAmplitude, "sine-amplitude", 1.0, "Sine amplitude (1.0 is a flatline). Reactive mode only")
	flag.IntVar(&c.SineDuration, "sine-duration", 150, "Half-cycle duration (150 seconds). Reactive mode only")
	flag.StringVar(&c.Username, "username", "scylla", "Username (scylla)")
	flag.StringVar(&c.Password, "password", "", "Password (SCYLLA_PASSWORD env, default: scylla)")
	flag.IntVar(&c.TabletsPerShard, "tablets-per-shard", 64, "Tablets per shard (64)")
	flag.IntVar(&c.TargetShardOps, "target-shard-ops", 5000, "Target ops per shard (5000)")
	flag.IntVar(&c.LogInterval, "log-interval", 15, "Log interval (15 seconds)")
	flag.StringVar(&c.KeyspaceName, "keyspace", "k", "Keyspace name")
	flag.StringVar(&c.TableName, "table", "t", "Table name")

	flag.Usage = func() {
                fmt.Fprintf(os.Stderr, "MODES\n")
                fmt.Fprintf(os.Stderr, "  reactive: Generate traffic pattern based on cluster capacity\n")
                fmt.Fprintf(os.Stderr, "  api:      Perform scaling operations using ScyllaDB Cloud API\n\n")

                fmt.Fprintf(os.Stderr, "MANDATORY PARAMETERS:\n")
                fmt.Fprintf(os.Stderr, "  -mode string\n")
                fmt.Fprintf(os.Stderr, "        Operating mode: 'reactive' or 'api' (default \"reactive\")\n")
                fmt.Fprintf(os.Stderr, "  -node string\n")
                fmt.Fprintf(os.Stderr, "        ScyllaDB node address to connect to (default \"127.0.0.1\")\n\n")

                fmt.Fprintf(os.Stderr, "CLOUD MODE REQUIRED PARAMETERS:\n")
                fmt.Fprintf(os.Stderr, "  -token string\n")
                fmt.Fprintf(os.Stderr, "        ScyllaDB Cloud API token (can use SCYLLA_CLOUD_TOKEN env var)\n")
                fmt.Fprintf(os.Stderr, "  -cluster int\n")
                fmt.Fprintf(os.Stderr, "        ScyllaDB Cloud cluster ID\n")
                fmt.Fprintf(os.Stderr, "  -target-peak int\n")
                fmt.Fprintf(os.Stderr, "        Target peak operations per second for scaling\n\n")

                fmt.Fprintf(os.Stderr, "CLOUD MODE OPTIONAL PARAMETERS:\n")
                fmt.Fprintf(os.Stderr, "  -baseline-shard-ops int\n")
                fmt.Fprintf(os.Stderr, "        Baseline operations per shard (default 2500)\n")
                fmt.Fprintf(os.Stderr, "  -baseline-duration int\n")
                fmt.Fprintf(os.Stderr, "        Duration in minutes to sustain baseline traffic before scaling (default 5)\n")
                fmt.Fprintf(os.Stderr, "  -peak-duration int\n")
                fmt.Fprintf(os.Stderr, "        Duration in minutes to sustain peak traffic before downscaling (default 5)\n")
                fmt.Fprintf(os.Stderr, "  -scaling-duration int\n")
                fmt.Fprintf(os.Stderr, "        Duration (minutes) it takes for traffic to scale in API mode. (default 10)\n")
                fmt.Fprintf(os.Stderr, "        For example:\n")
                fmt.Fprintf(os.Stderr, "           1. After 'baseline-duration' elapses, it takes 'scaling-duration' time " +
                                       "to reach the current topology peak.\n")
                fmt.Fprintf(os.Stderr, "           2. As scaling completes, 'scaling-duration' will take place to reach the " +
                                       "defined 'target-peak'\n")
                fmt.Fprintf(os.Stderr, "           3. When 'peak-duration' elapses, traffic ramps down within 'scaling-duration'" +
                                      "back to its original peak.\n\n")

                fmt.Fprintf(os.Stderr, "REACTIVE MODE PARAMETERS: \n")
                fmt.Fprintf(os.Stderr, "  -sine-amplitude float\n")
                fmt.Fprintf(os.Stderr, "        1.0 (default) indicates traffic targets the current topology peak (flatline). " +
                                       "<=1.0 indicates traffic will fluctuate as a sine\n")
                fmt.Fprintf(os.Stderr, "  -sine-duration int\n")
                fmt.Fprintf(os.Stderr, "        Half sine cycle in seconds (default 150). " +
                                       "How long it takes from sine baseline to peak[1.0] (and vice-versa)\n\n")

                fmt.Fprintf(os.Stderr, "GLOBAL PARAMETERS:\n")
                fmt.Fprintf(os.Stderr, "  -keyCount int\n")
                fmt.Fprintf(os.Stderr, "        Maximum number of unique keys to generate. Has a direct impact on storage utilization.\n")
                fmt.Fprintf(os.Stderr, "  -username string\n")
                fmt.Fprintf(os.Stderr, "        ScyllaDB username (default \"scylla\")\n")
                fmt.Fprintf(os.Stderr, "  -password string\n")
                fmt.Fprintf(os.Stderr, "        ScyllaDB password (SCYLLA_PASSWORD env, defaults to \"scylla\")\n")
                fmt.Fprintf(os.Stderr, "  -tablets-per-shard int\n")
                fmt.Fprintf(os.Stderr, "        Number of tablets per shard (default 64)\n")
                fmt.Fprintf(os.Stderr, "  -target-shard-ops int\n")
                fmt.Fprintf(os.Stderr, "        Target operations per shard for capacity calculations (default 5000)\n")
                fmt.Fprintf(os.Stderr, "  -log-interval int\n")
                fmt.Fprintf(os.Stderr, "        Logging interval in seconds (default 15s)\n")
                fmt.Fprintf(os.Stderr, "  -keyspace string\n")
                fmt.Fprintf(os.Stderr, "        Keyspace name for the test table (default \"k\")\n")
                fmt.Fprintf(os.Stderr, "  -table string\n")
                fmt.Fprintf(os.Stderr, "        Table name for the test table (default \"t\")\n\n")

                fmt.Fprintf(os.Stderr, "ENVIRONMENT VARIABLES:\n")
                fmt.Fprintf(os.Stderr, "  SCYLLA_CLOUD_TOKEN    ScyllaDB Cloud API token (alternative to -token)\n")
                fmt.Fprintf(os.Stderr, "  SCYLLA_PASSWORD       ScyllaDB password (alternative to -password)\n\n")

                fmt.Fprintf(os.Stderr, "EXAMPLES:\n")
                fmt.Fprintf(os.Stderr, "  Reactive mode:\n")
                fmt.Fprintf(os.Stderr, "    %s -mode reactive -node node-0.cluster.scylla.cloud\n\n", os.Args[0])
                fmt.Fprintf(os.Stderr, "  API scaling mode:\n")
                fmt.Fprintf(os.Stderr, "    %s -mode api -node node-0.cluster.scylla.cloud -token any -cluster 12345 -target-peak 90000\n\n", os.Args[0])
                fmt.Fprintf(os.Stderr, "    export SCYLLA_CLOUD_TOKEN=your-token-here\n")
                fmt.Fprintf(os.Stderr, "    %s -mode api -node node-0.cluster.scylla.cloud -cluster 12345 -target-peak 90000\n", os.Args[0])
        }
	flag.Parse()

	if c.Token == "" { c.Token = os.Getenv("SCYLLA_CLOUD_TOKEN") }
	if c.Password == "" { 
		if p := os.Getenv("SCYLLA_PASSWORD"); p != "" { c.Password = p } else { c.Password = "scylla" }
	}
	
	c.Mode = strings.ToLower(c.Mode)
	if c.Mode != "reactive" && c.Mode != "api" {
		log.Fatalf("Invalid mode: %s", c.Mode)
	}
	if c.Mode == "api" && (c.Token == "" || c.ClusterID == 0 || c.TargetPeakOps == 0) {
		log.Fatal("API mode requires -token, -cluster, and -target-peak")
	}
	if c.SineAmplitude < 0.1 || c.SineAmplitude > 1.0 {
		log.Fatalf("sine-amplitude must be 0.1-1.0, got %f", c.SineAmplitude)
	}
	return c
}

func (app *App) initDB() {
	cluster := gocql.NewCluster(app.config.Node)
	cluster.Logger = quietLogger{}
	cluster.Keyspace = "system"
	cluster.Consistency = gocql.LocalQuorum
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: app.config.Username, 
		Password: app.config.Password,
	}
	
	session, err := cluster.CreateSession()
	if err != nil { log.Fatalf("DB connection failed: %v", err) }
	
	// Get local DC
	var dc string
	session.Query("SELECT data_center FROM system.local").Consistency(gocql.LocalOne).Scan(&dc)
	if dc == "" { dc = "datacenter1" }
	
	// Create schema
	ksStmt := fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', '%s': 3} AND tablets = {'enabled': true};`, app.config.KeyspaceName, dc)
	if err := session.Query(ksStmt).Exec(); err != nil { log.Fatalf("Keyspace creation failed: %v", err) }
	
	tblStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (key blob PRIMARY KEY, val blob) WITH tablets = {'min_per_shard_tablet_count': %d};`, 
		app.config.KeyspaceName, app.config.TableName, app.config.TabletsPerShard)
	if err := session.Query(tblStmt).Exec(); err != nil { log.Fatalf("Table creation failed: %v", err) }
	
	// Get table ID
	session.Query(`SELECT id FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?`, 
		app.config.KeyspaceName, app.config.TableName).Scan(&app.tableId)
	
	// Switch to app keyspace
	cluster.Keyspace = app.config.KeyspaceName
	fallback := gocql.DCAwareRoundRobinPolicy(dc)
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)
	session.Close()
	
	app.session, err = cluster.CreateSession()
	if err != nil { log.Fatalf("App session failed: %v", err) }
}

func (app *App) initCloudAPI() {
	app.apiClient = &http.Client{Timeout: 30 * time.Second}
	
	// Get account ID
	req, _ := http.NewRequest("GET", "https://api.cloud.scylladb.com/account/default", nil)
	req.Header.Set("Authorization", "Bearer "+app.config.Token)
	resp, err := app.apiClient.Do(req)
	if err != nil { log.Fatalf("API request failed: %v", err) }
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		log.Fatalf("Get account API failed with status %d: %s", resp.StatusCode, string(body))
	}
	
	var accountResp struct{ Data struct{ AccountId int `json:"accountId"` } `json:"data"` }
	if err := json.NewDecoder(resp.Body).Decode(&accountResp); err != nil || accountResp.Data.AccountId == 0 {
		log.Fatal("Failed retrieving Cloud account details. Invalid token?")
	}
	app.accountId = accountResp.Data.AccountId
	
	// Get cluster details
	url := fmt.Sprintf("https://api.cloud.scylladb.com/account/%d/cluster/%d", app.accountId, app.config.ClusterID)
	req, _ = http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer "+app.config.Token)
	resp, err = app.apiClient.Do(req)
	if err != nil { log.Fatalf("API request failed: %v", err) } 
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		log.Fatalf("Get account API failed with status %d: %s", resp.StatusCode, string(body))
	}
	
	var clusterResp struct{ Data struct{ Cluster struct{ Dc struct{ Id, InstanceId int } } } }
	if err := json.NewDecoder(resp.Body).Decode(&clusterResp); err != nil || clusterResp.Data.Cluster.Dc.Id == 0 {
		log.Fatal("Unable to retrieve datacenter or instance Id. Invalid Cluster ID?\n" +
		    "Note: this tool does not support multi-region clusters")
	}
	app.dcId = clusterResp.Data.Cluster.Dc.Id
	app.instanceTypeId = clusterResp.Data.Cluster.Dc.InstanceId
	
	fmt.Printf("[Cloud API] AccountID: %d, DCID: %d, InstanceID: %d\n", app.accountId, app.dcId, app.instanceTypeId)
}

// TODO: Handle errors, we just accept whatever :(
func (app *App) scaleCluster(nodes int) {
	url := fmt.Sprintf("https://api.cloud.scylladb.com/account/%d/cluster/%d/resize", app.accountId, app.config.ClusterID)
	reqData := map[string]interface{}{
		"dcNodes": []map[string]int{{"dcId": app.dcId, "wantedSize": nodes, "instanceTypeId": app.instanceTypeId}},
	}
	jsonData, _ := json.Marshal(reqData)
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	req.Header.Set("Authorization", "Bearer "+app.config.Token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.apiClient.Do(req)
	if err != nil {
		log.Fatalf("[Cloud API] Failed scaling to %d nodes: %v\n", nodes, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >=300 {
		log.Fatalf("[Cloud API] Scaling request failed with status %d\n", resp.StatusCode)
	}
	fmt.Printf("[Cloud API] Scaling to %d nodes\n", nodes)
}

func (app *App) getClusterState() ClusterState {
	var nodes, shards, decommNodes int
	app.session.Query(`SELECT COUNT(1), SUM(shard_count) FROM system.topology WHERE key='topology' AND node_state='normal' ALLOW FILTERING`).Scan(&nodes, &shards)
	app.session.Query(`SELECT COUNT(1) FROM system.topology WHERE key='topology' AND node_state='decommissioning' ALLOW FILTERING`).Scan(&decommNodes)
	
	var tabletCount int
	app.session.Query(`SELECT tablet_count FROM system.tablets WHERE table_id = ? LIMIT 1`, app.tableId).Scan(&tabletCount)
	balanced := tabletCount >= (shards*app.config.TabletsPerShard)/3
	
	// Get min rack shards
	var minShards = math.MaxInt
	rackShards := make(map[string]int)
	iter := app.session.Query(`SELECT shard_count, rack FROM system.topology WHERE key='topology' AND node_state='normal' ALLOW FILTERING`).Iter()
	var shardCount int; var rack string
	for iter.Scan(&shardCount, &rack) { rackShards[rack] += shardCount }
	iter.Close()
	for _, count := range rackShards { if count < minShards { minShards = count } }
	if minShards == math.MaxInt { minShards = 0 }
	
	return ClusterState{nodes, shards, minShards, decommNodes, balanced}
}

func (app *App) initCapacity() {
	state := app.getClusterState()
	app.lastShardCount = state.Shards
	
	if app.config.Mode == "api" {
		app.originalNodes = state.Nodes
		app.originalShards = state.Shards
		app.currentBaseline = state.Shards * app.config.BaselineOpsPerShard
		app.currentPeak = state.Shards * app.config.TargetShardOps

		// Ensure we never scale if the cluster can already handle the target peak
		if app.currentPeak >= app.config.TargetPeakOps {
			log.Fatalf("Existing cluster capacity %d exceeds target peak %d. Review -target-shard-ops and -target-peak",
			    app.currentPeak, app.config.TargetPeakOps)
		}
		
		opsPerNode := app.currentPeak / state.Nodes
		requiredNodes := int(math.Ceil(float64(app.config.TargetPeakOps) / float64(opsPerNode)))
		app.scaleNodes = ((requiredNodes + 2) / 3) * 3
		app.phaseStartTime = time.Now()
		
		fmt.Printf("[Scaling Init] %d nodes, %d shards, baseline: %d, target: %d, scale to: %d\n",
			state.Nodes, state.Shards, app.currentBaseline, app.config.TargetPeakOps, app.scaleNodes)
	} else {
		app.currentPeak = state.MinRackShards * 3 * app.config.TargetShardOps
		app.currentBaseline = int(app.config.SineAmplitude * float64(app.currentPeak))
		fmt.Printf("[Reactive Init] %d shards, peak: %d, baseline: %d\n", state.Shards, app.currentPeak, app.currentBaseline)
	}
}

func (app *App) generateTraffic(elapsed time.Duration) int {
	if app.config.Mode == "api" {
		return app.generateScalingTraffic()
	}
	return app.generateReactiveTraffic(elapsed)
}

func (app *App) generateReactiveTraffic(elapsed time.Duration) int {
	cycleDuration := time.Duration(app.config.SineDuration*2) * time.Second
	t := float64(elapsed.Seconds())/cycleDuration.Seconds() + app.timeOffset
	
	if app.transitionPending {
		normalized := 0.5 * (1 - math.Cos(2*math.Pi*t))
		currentTraffic := float64(app.currentBaseline) + normalized*float64(app.currentPeak-app.currentBaseline)
		
		if currentTraffic >= float64(app.newBaseline) && currentTraffic <= float64(app.newCapacity) {
			newNormalized := (currentTraffic - float64(app.newBaseline)) / float64(app.newCapacity-app.newBaseline)
			newNormalized = math.Max(0, math.Min(1, newNormalized))
			cosValue := math.Max(-1, math.Min(1, 1-2*newNormalized))
			newT := math.Acos(cosValue) / (2 * math.Pi)
			if math.Sin(2*math.Pi*t) < 0 { newT = 1 - newT }
			
			app.timeOffset = newT - float64(elapsed.Seconds())/cycleDuration.Seconds()
			app.currentBaseline, app.currentPeak = app.newBaseline, app.newCapacity
			app.transitionPending = false
		} else {
			rangesOverlap := !(app.currentPeak < app.newBaseline || app.newCapacity < app.currentBaseline)
			// Ranges don't overlap, transition immediately
			if !rangesOverlap {
				app.timeOffset = 0
				app.currentBaseline, app.currentPeak = app.newBaseline, app.newCapacity
				app.transitionPending = false
			}
		}
	}
	
	normalized := 0.5 * (1 - math.Cos(2*math.Pi*t))
	return int(float64(app.currentBaseline) + normalized*float64(app.currentPeak-app.currentBaseline))
}

func (app *App) generateScalingTraffic() int {
	phaseElapsed := time.Since(app.phaseStartTime)
	scaleDuration := time.Duration(app.config.ScalingDuration) * time.Minute
	
	ramp := func(start, end int) int {
		progress := math.Min(1.0, float64(phaseElapsed)/float64(scaleDuration))
		sineProgress := 0.5 * (1 - math.Cos(math.Pi*progress))
		return int(float64(start) + sineProgress*float64(end-start))
	}
	
	switch app.scalingPhase {
	case Baseline: return app.currentBaseline
	case PreScale: return ramp(app.currentBaseline, app.currentPeak)
	case Scaling: return app.currentPeak
	case PostScale: return ramp(app.postScaleStartTraffic, app.config.TargetPeakOps)
	case Peak: return app.config.TargetPeakOps
	case ScaleDown: return ramp(app.config.TargetPeakOps, app.originalShards*app.config.TargetShardOps)
	case Decommission: return app.currentBaseline
	default: return app.currentBaseline
	}
}

func (app *App) updateScaling(state ClusterState, currentTraffic int) {
	phaseElapsed := time.Since(app.phaseStartTime)
	prevPhase := app.scalingPhase
	
	switch app.scalingPhase {
	case Baseline:
		if phaseElapsed >= time.Duration(app.config.BaselineDuration)*time.Minute {
			go app.scaleCluster(app.scaleNodes)
			app.scaleUpTriggered = true
			app.scalingPhase = PreScale
			app.phaseStartTime = time.Now()
		}
	case PreScale:
		if app.scaleUpTriggered && state.Nodes >= app.scaleNodes && state.Balanced {
			app.currentPeak = state.Shards * app.config.TargetShardOps
			app.postScaleStartTraffic = currentTraffic
			app.scalingPhase = PostScale
			app.phaseStartTime = time.Now()
		} else if phaseElapsed >= time.Duration(app.config.ScalingDuration)*time.Minute {
			app.scalingPhase = Scaling
			app.phaseStartTime = time.Now()
		}
	case Scaling:
		if state.Nodes >= app.scaleNodes && state.Balanced {
			app.currentPeak = state.Shards * app.config.TargetShardOps
			app.postScaleStartTraffic = currentTraffic
			app.scalingPhase = PostScale
			app.phaseStartTime = time.Now()
		}
	case PostScale:
		if currentTraffic >= app.config.TargetPeakOps {
			app.scalingPhase = Peak
			app.phaseStartTime = time.Now()
		}
	case Peak:
		if phaseElapsed >= time.Duration(app.config.PeakDuration)*time.Minute {
			go app.scaleCluster(app.originalNodes)
			app.scaleDownTriggered = true
			app.scalingPhase = ScaleDown
			app.phaseStartTime = time.Now()
		}
	case ScaleDown:
		if currentTraffic <= app.originalShards*app.config.TargetShardOps && app.scaleDownTriggered {
			app.scalingPhase = Decommission
			app.phaseStartTime = time.Now()
		}
	case Decommission:
		if state.Nodes <= app.originalNodes && state.Balanced && state.DecommissioningNodes == 0 {
			app.scalingPhase = Baseline
			app.phaseStartTime = time.Now()
			app.scaleUpTriggered, app.scaleDownTriggered = false, false
			app.currentPeak = state.Shards * app.config.TargetShardOps
			app.currentBaseline = state.Shards * app.config.BaselineOpsPerShard
		}
	}
	
	if prevPhase != app.scalingPhase {
		fmt.Printf("[Phase] %s → %s\n", prevPhase, app.scalingPhase)
	}
}

func (app *App) updateReactive(state ClusterState) {
	app.newCapacity = state.MinRackShards * 3 * app.config.TargetShardOps
	if state.Shards != app.lastShardCount {
		app.newBaseline = int(app.config.SineAmplitude * float64(app.newCapacity))
		if (app.newCapacity > app.currentPeak && state.Balanced) || app.newCapacity < app.currentPeak {
			fmt.Printf("[Capacity Change] %d→%d shards, baseline: %d→%d, peak: %d→%d\n",
				app.lastShardCount, state.Shards, app.currentBaseline, app.newBaseline, app.currentPeak, app.newCapacity)
			app.transitionPending = true
			app.lastShardCount = state.Shards
		}
	}
}

func (app *App) sendWrite() {
	keyId := atomic.AddUint64(&app.keyCounter, 1) % uint64(app.config.KeyCount)
	key := make([]byte, 16)
	binary.LittleEndian.PutUint64(key, keyId)
	binary.LittleEndian.PutUint64(key[8:], keyId)

	val := app.valPool[keyId % uint64(len(app.valPool))]
	query := fmt.Sprintf("INSERT INTO %s.%s (key, val) VALUES (?, ?)", app.config.KeyspaceName, app.config.TableName)
	if err := app.session.Query(query, key, val).Exec(); err != nil {
		atomic.AddUint64(&app.failureCount, 1)
	} else {
		atomic.AddUint64(&app.successCount, 1)
	}
}

func (app *App) generateWrites(rps int) {
	workers := runtime.NumCPU()
	batchSize := rps / workers
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < batchSize; j++ { app.sendWrite() }
		}()
	}
	wg.Wait()
}

func (app *App) startMonitors() {
	// Throughput monitor
	go func() {
		var prevSuccess, prevFailure uint64
		for range time.Tick(time.Duration(app.config.LogInterval) * time.Second) {
			succ, fail := atomic.LoadUint64(&app.successCount), atomic.LoadUint64(&app.failureCount)
			fmt.Printf("[Throughput] Success: %d/sec, Failures: %d/sec\n", (succ-prevSuccess)/uint64(app.config.LogInterval), (fail-prevFailure)/uint64(app.config.LogInterval))
			prevSuccess, prevFailure = succ, fail
		}
	}()
}

func (app *App) initPools(valCount int) {
	app.valPool = make([][]byte, valCount)
	for i := range app.valPool {
		buf := make([]byte, 512)
		rand.Read(buf)
		app.valPool[i] = buf
	}
}

func main() {
	app := &App{config: parseFlags()}
	defer func() { if app.session != nil { app.session.Close() } }()
	
	app.initDB()
	app.initPools(1_000_000) // Pre-generate 1M random 512B values
	if app.config.Mode == "api" { app.initCloudAPI() }
	app.initCapacity()
	app.startMonitors()
	
	start, lastLog := time.Now(), time.Time{}
	for {
		elapsed := time.Since(start)
		targetRPS := app.generateTraffic(elapsed)
		
		if time.Since(lastLog) >= time.Duration(app.config.LogInterval)*time.Second {
			if app.config.Mode == "api" {
				fmt.Printf("[Traffic] %v, %d rps, Phase: %s\n", elapsed.Round(time.Second), targetRPS, app.scalingPhase)
			} else {
				fmt.Printf("[Traffic] %v, %d rps\n", elapsed.Round(time.Second), targetRPS)
			}
			lastLog = time.Now()
		}
		
		go app.generateWrites(targetRPS)
		
		if int(elapsed.Seconds())%15 == 0 {
			state := app.getClusterState()
			if app.config.Mode == "api" {
				app.updateScaling(state, targetRPS)
			} else {
				app.updateReactive(state)
			}
		}
		
		time.Sleep(time.Second)
	}
}
