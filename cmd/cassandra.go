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
	cassandraFixer bool
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

		var consumerId string
		var consumerUsername string
		fmt.Println("Checking consumers...")
		query := session.Query("SELECT id, username FROM consumers")
		query.PageSize(1)
		iter := query.Iter()
		for iter.Scan(&consumerId, &consumerUsername) {
			//fmt.Println("id:", consumerId,"username:", consumerUsername)
			if countWorkspaceEntities(consumerId,session) == 0 {
				if !cassandraFixer {
					return errors.New("Orphaned consumer: " + consumerId)
				}
			}
		}

		var keyAuthCredentialsId string
		var keyAuthCredentialsConsumerId string
		fmt.Println("Checking keyauth_credentials...")
		query = session.Query("SELECT id, consumer_id FROM keyauth_credentials")
		query.PageSize(1)
		iter = query.Iter()
		for iter.Scan(&keyAuthCredentialsId, &keyAuthCredentialsConsumerId) {
			//fmt.Println("consumer_id:", keyAuthCredentialsConsumerId)
			if countConsumers(keyAuthCredentialsConsumerId,session) == 0 {
				if !cassandraFixer {
					return errors.New("Orphaned keyauth_credentials: " + keyAuthCredentialsId)
				}else{
					fmt.Println("Cleaning keyauth_credentials:", keyAuthCredentialsId)
					if err := session.Query("DELETE FROM keyauth_credentials WHERE id = ?", keyAuthCredentialsId).Exec(); err != nil {
						return errors.New("Error while deleting keyauth_credentials: " + keyAuthCredentialsId)
					}
				}
			}
		}

		var pluginsId string
		fmt.Println("Checking plugins...")
		query = session.Query("SELECT id FROM plugins")
		query.PageSize(1)
		iter = query.Iter()
		for iter.Scan(&pluginsId) {
			//fmt.Println("id:", pluginsId)
			if countWorkspaceEntities(pluginsId,session) != 1 {
				if !cassandraFixer {
					return errors.New("Orphaned plugins: " + pluginsId)
				}else{
					fmt.Println("Cleaning plugins:", pluginsId)
					if err := session.Query("DELETE FROM plugins WHERE id = ?", pluginsId).Exec(); err != nil {
						return errors.New("Error while deleting plugins: " + pluginsId)
					}
				}
			}
		}

		var routesId string
		fmt.Println("Checking routes...")
		query = session.Query("SELECT id FROM routes")
		query.PageSize(1)
		iter = query.Iter()
		for iter.Scan(&routesId) {
			//fmt.Println("id:", routesId)
			if countWorkspaceEntities(routesId,session) != 2 {
				if !cassandraFixer {
					return errors.New("Orphaned routes: " + routesId)
				}else{
					fmt.Println("Cleaning routes:", routesId)
					if err := session.Query("DELETE FROM routes WHERE id = ? and partition = 'routes'", routesId).Exec(); err != nil {
						return errors.New("Error while deleting routes: " + routesId)
					}
				}
			}
		}

		var servicesId string
		fmt.Println("Checking services...")
		query = session.Query("SELECT id FROM services")
		query.PageSize(1)
		iter = query.Iter()
		for iter.Scan(&servicesId) {
			//fmt.Println("id:", servicesId)
			if countWorkspaceEntities(servicesId,session) != 2 {
				if !cassandraFixer {
					return errors.New("Orphaned services: " + servicesId)
				}else{
					//fmt.Println("Cleaning keyauth_credentials:", keyAuthCredentialsId)
					//if err := session.Query("DELETE FROM keyauth_credentials WHERE id = ?", keyAuthCredentialsId).Exec(); err != nil {
					//	return errors.New("Error while deleting keyauth_credentials: " + keyAuthCredentialsId)
					//}
				}
			}
		}

		//var filesId string
		//fmt.Println("Checking files (this will take long)...")
		//query = session.Query("SELECT id FROM files")
		//query.PageSize(1)
		//iter = query.Iter()
		//for iter.Scan(&filesId) {
		//	//fmt.Println("id:", filesId)
		//	if countWorkspaceEntities(filesId,session) != 2 {
		//		if !cassandraFixer {
		//			return errors.New("Orphaned files: " + servicesId)
		//		}else{
		//			//fmt.Println("Cleaning keyauth_credentials:", keyAuthCredentialsId)
		//			//if err := session.Query("DELETE FROM keyauth_credentials WHERE id = ?", keyAuthCredentialsId).Exec(); err != nil {
		//			//	return errors.New("Error while deleting keyauth_credentials: " + keyAuthCredentialsId)
		//			//}
		//		}
		//	}
		//}

		fmt.Println("Integrity check complete.")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(cassandraCmd)
	cassandraCmd.Flags().StringVarP(&cassandraKeyspace, "keyspace", "k",
		"", "Check orphaned resources in cassandra database")
	cassandraCmd.Flags().BoolVar(&cassandraFixer, "fix", false,
		"Delete orphaned resources. PLEASE DO NOT USE IF NOT STRICTLY NECESSARY")
}

func countWorkspaceEntities(entityId string, session *gocql.Session) int{
	var count int
	iter := session.Query("select count(*) as count from workspace_entities where entity_id = ? allow filtering;",entityId).Iter()
	for iter.Scan(&count) {
		//fmt.Println("	Counted entities:",count)
	}
	return count
}

func countConsumers(id string, session *gocql.Session) int{
	var count int
	iter := session.Query("select count(*) as count from consumers where id = ?;",id).Iter()
	for iter.Scan(&count) {
		//fmt.Println("	Counted entities:",count)
	}
	return count
}
