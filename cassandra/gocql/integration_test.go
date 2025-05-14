//go:build integration
// +build integration

/*
Copyright 2025 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package spanner

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
)

var (
	cluster     *gocql.ClusterConfig
	keyspace    = "it_test"
	tableName   = "AllCqlConstantTypes"
	databaseUri = ""
	env         = "spanner"
)

var adminClient *database.DatabaseAdminClient

// Sample command to run it test:
// go test -v  ./... -tags=integration
func TestMain(m *testing.M) {
	var target string
	flag.StringVar(
		&target,
		"target",
		"spanner",
		"Specify the test target: 'spanner' or 'cassandra'",
	)
	var spannerEndpoint string
	flag.StringVar(
		&spannerEndpoint,
		"spanner-endpoint",
		"spanner.googleapis.com:443",
		"Specify the spanner endpoint, default to spanner.googleapis.com:443",
	)
	flag.Parse()

	if target == "" {
		env = "spanner"
	} else {
		env = target
	}
	var err error
	keyspace, err = generateKeySpaceName()
	if err != nil {
		log.Fatalf(err.Error())
	}

	switch target {
	case "spanner":
		code := setupAndRunSpanner(m, spannerEndpoint)
		os.Exit(code)
	case "cassandra":
		code := setupAndRunCassandra(m)
		os.Exit(code)
	default:
		log.Fatalf("Invalid target - %s", target)
	}
}

func generateKeySpaceName() (string, error) {
	const (
		keyspacePrefix = "gocql_test_"
		// Go's reference time: Mon Jan 2 15:04:05 MST 2006
		// Format YYYYMMDDHHMMSS
		timestampFormat = "20060102150405"
	)

	currentTime := time.Now()
	timestampStr := currentTime.Format(timestampFormat)
	keyspace := fmt.Sprintf("%s%s", keyspacePrefix, timestampStr)
	return keyspace, nil
}

func setupAndRunSpanner(m *testing.M, spannerEndpoint string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	instanceURI := os.Getenv("INTEGRATION_TEST_INSTANCE")
	if instanceURI == "" {
		log.Fatalf(
			"environment variable INTEGRATION_TEST_INSTANCE is not set or is empty",
		)
	}
	databaseUri = fmt.Sprintf("%s/databases/%s", instanceURI, keyspace)
	var err error
	adminClient, err = database.NewDatabaseAdminClient(ctx, []option.ClientOption{
		option.WithEndpoint(spannerEndpoint),
	}...)
	if err != nil {
		log.Fatalf(err.Error())
	}
	// Create a new random testing database
	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          instanceURI,
		CreateStatement: "CREATE DATABASE `" + keyspace + "`",
	})
	if err != nil {
		log.Fatalf(err.Error())
	}
	if _, err := op.Wait(ctx); err != nil {
		log.Fatalf(err.Error())
	}
	// Drop testing database
	defer func() {
		dropCtx, dropCancel := context.WithTimeout(
			context.Background(),
			2*time.Minute,
		)
		defer dropCancel()
		if err := adminClient.DropDatabase(dropCtx,
			&adminpb.DropDatabaseRequest{Database: databaseUri}); err != nil {
			log.Fatalf("failed to drop testing database %v: %v", databaseUri, err)
		}
		defer adminClient.Close()
	}()

	opts := &Options{
		DatabaseUri:     databaseUri,
		SpannerEndpoint: spannerEndpoint,
	}

	cluster = NewCluster(opts)
	if cluster == nil {
		log.Fatalf("Failed to create cluster")
	}
	cluster.Keyspace = keyspace
	defer CloseCluster(cluster)

	// Run the tests
	return m.Run()
}

func setupAndRunCassandra(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	// Define the container request
	req := testcontainers.ContainerRequest{
		Image:        "cassandra:latest",
		ExposedPorts: []string{"9042/tcp"}, // Expose the default Cassandra CQL port
		Env: map[string]string{
			"MAX_HEAP_SIZE": "512M", // Optional tuning for Cassandra
			"HEAP_NEWSIZE":  "100M",
		},
		WaitingFor: wait.ForLog("Starting listening for CQL clients on /0.0.0.0:9042").
			WithStartupTimeout(120 * time.Second),

		// Wait until Cassandra is ready
	}

	// Start the Cassandra container
	cassandraContainer, err := testcontainers.GenericContainer(
		ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		},
	)
	if err != nil {
		log.Fatalf("Could not start container: %v", err)
	}
	defer func() {
		// Ensure the container is terminated after tests
		if err := cassandraContainer.Terminate(ctx); err != nil {
			log.Fatalf("Error while terminating container - %v", err)
		}
	}()

	// Get the container host and mapped port for Cassandra
	host, err := cassandraContainer.Host(ctx)
	if err != nil {
		log.Fatalf("Could not get container host: %v", err)
	}

	mappedPort, err := cassandraContainer.MappedPort(ctx, "9042")
	if err != nil {
		log.Fatalf("Could not get mapped port: %v", err)
	}

	// Configure and create a new Cassandra cluster session
	cluster = gocql.NewCluster(host)
	cluster.Port = mappedPort.Int()           // Use the mapped port from the container
	cluster.ProtoVersion = 4                  // Use protocol version 4
	cluster.ConnectTimeout = 30 * time.Second // Set connection timeout

	// Create a session with Cassandra
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Could not connect to Cassandra: %v", err)
	}
	defer session.Close()
	//create Kespace
	ddlStatementKeyspace := fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' }",
		keyspace,
	)
	if err := session.Query(ddlStatementKeyspace).Exec(); err != nil {
		log.Fatalf("Could not create keyspace: %v", err)
	}
	cluster.Keyspace = keyspace
	// Run the tests
	return m.Run()
}

func createCqlTable(s *gocql.Session, table string) error {
	if err := s.Query(table).RetryPolicy(&gocql.SimpleRetryPolicy{}).Exec(); err != nil {
		log.Printf("error creating table table=%q err=%v\n", table, err)
		return err
	}
	return nil
}

func createSpannerTable(table string) error {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Minute,
	)
	defer cancel()
	op, err := adminClient.UpdateDatabaseDdl(
		ctx,
		&adminpb.UpdateDatabaseDdlRequest{
			Database: databaseUri,
			Statements: []string{
				table,
			},
		},
	)
	if err != nil {
		return err
	}
	if err := op.Wait(ctx); err != nil {
		return err
	}
	return nil
}

func TestBatch(t *testing.T) {
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to create cql session: %v", err)
	}
	defer session.Close()

	if env == "spanner" {
		if err := createSpannerTable(`CREATE TABLE batch_table (id INT64 NOT NULL OPTIONS (cassandra_type = 'int'))PRIMARY KEY (id)`); err != nil {
			t.Fatal("create spanner table:", err)
		}
	} else {
		if err := createCqlTable(session, `CREATE TABLE batch_table (id int primary key)`); err != nil {
			t.Fatal("create cassandra table:", err)
		}
	}

	batch := session.NewBatch(gocql.LoggedBatch)
	for i := 0; i < 10; i++ {
		batch.Query(`INSERT INTO batch_table (id) VALUES (?)`, i)
	}

	if err := session.ExecuteBatch(batch); err != nil {
		t.Fatal("execute batch:", err)
	}

	count := 0
	if err := session.Query(`SELECT COUNT(*) FROM batch_table`).Scan(&count); err != nil {
		t.Fatal("select count:", err)
	} else if count != 10 {
		t.Fatalf("count: expected %d, got %d\n", 100, count)
	}
}

func TestSmallInt(t *testing.T) {
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to create cql session: %v", err)
	}
	defer session.Close()

	if env == "spanner" {
		if err := createSpannerTable(`CREATE TABLE smallint_table (id INT64 NOT NULL OPTIONS (cassandra_type = 'smallint'))PRIMARY KEY (id)`); err != nil {
			t.Fatal("create spanner table:", err)
		}
	} else {
		if err := createCqlTable(session, `CREATE TABLE smallint_table (id smallint primary key)`); err != nil {
			t.Fatal("create cassandra table:", err)
		}
	}

	m := make(map[string]interface{})
	m["id"] = int16(2)
	sliceMap := []map[string]interface{}{m}
	if err := session.Query(`INSERT INTO smallint_table (id) VALUES (?)`,
		m["id"]).Exec(); err != nil {
		t.Fatal("insert:", err)
	}
	if returned, retErr := session.Query(`SELECT * FROM smallint_table`).Iter().SliceMap(); retErr != nil {
		t.Fatal("select:", retErr)
	} else {
		if sliceMap[0]["id"] != returned[0]["id"] {
			t.Fatal("returned id did not match")
		}
	}
}

func TestScanWithNilArguments(t *testing.T) {
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to create cql session: %v", err)
	}
	defer session.Close()

	if env == "spanner" {
		if err := createSpannerTable(`CREATE TABLE scan_with_nil_arguments (
			foo STRING(MAX) OPTIONS (cassandra_type = 'varchar'),
			bar INT64 OPTIONS (cassandra_type = 'int'),
			) 
			PRIMARY KEY (foo, bar)`); err != nil {
			t.Fatal("create spanner table:", err)
		}
	} else {
		if err := createCqlTable(session, `CREATE TABLE scan_with_nil_arguments (
			foo   varchar,
			bar   int,
			PRIMARY KEY (foo, bar)
		)`); err != nil {
			t.Fatal("create cassandra table:", err)
		}
	}

	for i := 1; i <= 20; i++ {
		if err := session.Query("INSERT INTO scan_with_nil_arguments (foo, bar) VALUES (?, ?)",
			"squares", i*i).Exec(); err != nil {
			t.Fatal("insert:", err)
		}
	}

	iter := session.Query("SELECT * FROM scan_with_nil_arguments WHERE foo = ?", "squares").Iter()
	var n int
	count := 0
	for iter.Scan(nil, &n) {
		count += n
	}
	if err := iter.Close(); err != nil {
		t.Fatal("close:", err)
	}
	if count != 2870 {
		t.Fatalf("expected %d, got %d", 2870, count)
	}
}

func TestRebindQueryInfo(t *testing.T) {
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to create cql session: %v", err)
	}
	defer session.Close()

	if env == "spanner" {
		if err := createSpannerTable(`CREATE TABLE rebind_query (
			id INT64 NOT NULL OPTIONS (cassandra_type = 'int'),
			value STRING(MAX) NOT NULL OPTIONS (cassandra_type = 'text'),
			) 
			PRIMARY KEY (id)`); err != nil {
			t.Fatal("create spanner table:", err)
		}
	} else {
		if err := createCqlTable(session, `CREATE TABLE rebind_query (id int, value text, PRIMARY KEY (id))`); err != nil {
			t.Fatal("create cassandra table:", err)
		}
	}

	if err := session.Query("INSERT INTO rebind_query (id, value) VALUES (?, ?)", 23, "quux").Exec(); err != nil {
		t.Fatalf("insert into rebind_query failed, err '%v'", err)
	}

	if err := session.Query("INSERT INTO rebind_query (id, value) VALUES (?, ?)", 24, "w00t").Exec(); err != nil {
		t.Fatalf("insert into rebind_query failed, err '%v'", err)
	}

	q := session.Query("SELECT value FROM rebind_query WHERE ID = ?")
	q.Bind(23)

	iter := q.Iter()
	var value string
	for iter.Scan(&value) {
	}

	if value != "quux" {
		t.Fatalf("expected %v but got %v", "quux", value)
	}

	q.Bind(24)
	iter = q.Iter()

	for iter.Scan(&value) {
	}

	if value != "w00t" {
		t.Fatalf("expected %v but got %v", "w00t", value)
	}
}

func TestVarint(t *testing.T) {
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to create cql session: %v", err)
	}
	defer session.Close()

	if env == "spanner" {
		if err := createSpannerTable(`CREATE TABLE varint_test (
			id STRING(MAX) NOT NULL OPTIONS (cassandra_type = 'varchar'),
			test NUMERIC OPTIONS (cassandra_type = 'varint'),
			test2 NUMERIC OPTIONS (cassandra_type = 'varint'),
			) 
			PRIMARY KEY (id)`); err != nil {
			t.Fatal("create spanner table:", err)
		}
	} else {
		if err := createCqlTable(session, `CREATE TABLE varint_test (id varchar, test varint, test2 varint, primary key (id))`); err != nil {
			t.Fatal("create cassandra table:", err)
		}
	}

	if err := session.Query(`INSERT INTO varint_test (id, test) VALUES (?, ?)`, "id", 0).Exec(); err != nil {
		t.Fatalf("insert varint: %v", err)
	}

	var result int
	if err := session.Query("SELECT test FROM varint_test").Scan(&result); err != nil {
		t.Fatalf("select from varint_test failed: %v", err)
	}

	if result != 0 {
		t.Errorf("Expected 0, was %d", result)
	}

	if err := session.Query(`INSERT INTO varint_test (id, test) VALUES (?, ?)`, "id", nil).Exec(); err != nil {
		t.Fatalf("insert varint: %v", err)
	}

	if err := session.Query("SELECT test FROM varint_test").Scan(&result); err != nil {
		t.Fatalf("select from varint_test failed: %v", err)
	}

	if result != 0 {
		t.Errorf("Expected 0, was %d", result)
	}

	var nullableResult *int

	if err := session.Query("SELECT test FROM varint_test").Scan(&nullableResult); err != nil {
		t.Fatalf("select from varint_test failed: %v", err)
	}

	if nullableResult != nil {
		t.Errorf("Expected nil, was %d", nullableResult)
	}

	biggie := new(big.Int)
	biggie.SetString("36893488147419103232", 10) // > 2**64
	if err := session.Query(`INSERT INTO varint_test (id, test) VALUES (?, ?)`, "id", biggie).Exec(); err != nil {
		t.Fatalf("insert varint: %v", err)
	}

	resultBig := new(big.Int)
	if err := session.Query("SELECT test FROM varint_test").Scan(resultBig); err != nil {
		t.Fatalf("select from varint_test failed: %v", err)
	}

	if resultBig.String() != biggie.String() {
		t.Errorf("Expected %s, was %s", biggie.String(), resultBig.String())
	}

	// value not set in cassandra, leave bind variable empty
	resultBig = new(big.Int)
	if err := session.Query("SELECT test2 FROM varint_test").Scan(resultBig); err != nil {
		t.Fatalf("select from varint_test failed: %v", err)
	}

	if resultBig.Int64() != 0 {
		t.Errorf("Expected %s, was %s", biggie.String(), resultBig.String())
	}

	// can use double pointer to explicitly detect value is not set in cassandra
	if err := session.Query("SELECT test2 FROM varint_test").Scan(&resultBig); err != nil {
		t.Fatalf("select from varint_test failed: %v", err)
	}

	if resultBig != nil {
		t.Errorf("Expected %v, was %v", nil, *resultBig)
	}
}

func TestNilInQuery(t *testing.T) {
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to create cql session: %v", err)
	}
	defer session.Close()

	if env == "spanner" {
		if err := createSpannerTable(`CREATE TABLE testNilInsert (
			id INT64 OPTIONS (cassandra_type = 'int'),
			count_col INT64 OPTIONS (cassandra_type = 'int'),
			) 
			PRIMARY KEY (id)`); err != nil {
			t.Fatal("create spanner table:", err)
		}
	} else {
		if err := createCqlTable(session, `CREATE TABLE testNilInsert (id int, count_col int, PRIMARY KEY (id))`); err != nil {
			t.Fatal("create cassandra table:", err)
		}
	}

	if err := session.Query("INSERT INTO testNilInsert (id,count_col) VALUES (?,?)", 1, nil).Exec(); err != nil {
		t.Fatalf("failed to insert with err: %v", err)
	}

	var id int

	if err := session.Query("SELECT id FROM testNilInsert").Scan(&id); err != nil {
		t.Fatalf("failed to select with err: %v", err)
	} else if id != 1 {
		t.Fatalf("expected id to be 1, got %v", id)
	}
}

func TestEmptyTimestamp(t *testing.T) {
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to create cql session: %v", err)
	}
	defer session.Close()

	if env == "spanner" {
		if err := createSpannerTable(`CREATE TABLE test_empty_timestamp (
			id INT64 OPTIONS (cassandra_type = 'int'),
			time TIMESTAMP OPTIONS (cassandra_type = 'timestamp'),
			num INT64 OPTIONS (cassandra_type = 'int'),
			) 
			PRIMARY KEY (id)`); err != nil {
			t.Fatal("create spanner table:", err)
		}
	} else {
		if err := createCqlTable(session, `CREATE TABLE test_empty_timestamp (id int, time timestamp, num int, PRIMARY KEY (id))`); err != nil {
			t.Fatal("create cassandra table:", err)
		}
	}

	if err := session.Query("INSERT INTO test_empty_timestamp (id, num) VALUES (?,?)", 1, 561).Exec(); err != nil {
		t.Fatalf("failed to insert with err: %v", err)
	}

	var timeVal time.Time

	if err := session.Query("SELECT time FROM test_empty_timestamp where id = ?", 1).Scan(&timeVal); err != nil {
		t.Fatalf("failed to select with err: %v", err)
	}

	if !timeVal.IsZero() {
		t.Errorf("time.Time bind variable should still be empty (was %s)", timeVal)
	}
}

func TestLargeSizeQuery(t *testing.T) {
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to create cql session: %v", err)
	}
	defer session.Close()

	if env == "spanner" {
		if err := createSpannerTable(`CREATE TABLE large_size_query (
			id INT64 OPTIONS (cassandra_type = 'int'),
			text_col STRING(MAX) OPTIONS (cassandra_type = 'varchar'),
			) 
			PRIMARY KEY (id)`); err != nil {
			t.Fatal("create spanner table:", err)
		}
	} else {
		if err := createCqlTable(session, `CREATE TABLE IF NOT EXISTS large_size_query(id int, text_col text, PRIMARY KEY (id))`); err != nil {
			t.Fatal("create cassandra table:", err)
		}
	}

	longString := strings.Repeat("a", 500_000)

	err = session.Query("INSERT INTO large_size_query (id, text_col) VALUES (?, ?)", "1", longString).Exec()
	if err != nil {
		t.Fatal(err)
	}

	var result string
	err = session.Query("SELECT text_col FROM large_size_query").Scan(&result)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, longString, result)
}