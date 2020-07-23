package cmd

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/gocql/gocql"
	"time"
)

var (
	cassandraKeyspace  string
)

// syncCmd represents the sync command
var cassandraCmd = &cobra.Command{
	Use: "cassandra",
	Short: "Cassandra is used to connect directly to Cassandra Cluster and find Orphaned resources on a workspace",
	Long: `Cassandra is used to connect directly to Cassandra Cluster and find Orphaned resources on a workspace`,
	Args: validateNoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {

		cluster := gocql.NewCluster(config.CassandraContactPoint)
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: config.CassandraUsername,
			Password: config.CassandraPassword,
		}
		cluster.Keyspace = cassandraKeyspace
		cluster.Consistency = gocql.Quorum
		cluster.Timeout = 20 * time.Second
		session, err := cluster.CreateSession()
		if err != nil {
			return errors.Wrap(err, "Connection Error")
		}
		defer session.Close()

		var id string
		var username string
		fmt.Println("Checking consumers...")
		iter := session.Query("SELECT id, username FROM consumers").Iter()
		for iter.Scan(&id, &username) {
			fmt.Println("id:",id,"username:",username)
			checkWorkspaceEntities(id,session)
		}

		fmt.Println("The end.")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(cassandraCmd)
	cassandraCmd.Flags().StringVarP(&cassandraKeyspace, "keyspace", "k",
		"", "Check orphaned resources in cassandra database")
}

func checkWorkspaceEntities(entityId string, session *gocql.Session) int{
	var count int
	iter := session.Query("select count(*) as id from workspace_entities where entity_id = ? allow filtering;",entityId).Iter()
	for iter.Scan(&count) {
		fmt.Println("count:",count)
	}
	return count
}
