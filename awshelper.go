package main

import (
	"fmt"
	"math"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/rds"
)

func main() {
	sess, _ := createAwsSession("pd-production", "ap-southeast-1")
	//retrieveRDSSnapshotstats(sess)
	retrieveDDBTables(sess)
}

func createAwsSession(awsprofile string, region string) (session.Session, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewSharedCredentials("", awsprofile)},
	)
	if err != nil {
		fmt.Printf("Unable to create AWS session, %v", err)
	}
	return *sess, err
}

func retrieveRDSSnapshotstats(session session.Session) {
	client := rds.New(&session)

	var dbsnapshots = []*rds.DBClusterSnapshot{}
	qerr := client.DescribeDBClusterSnapshotsPages(&rds.DescribeDBClusterSnapshotsInput{},
		func(page *rds.DescribeDBClusterSnapshotsOutput, lastPage bool) bool {
			dbsnapshots = append(dbsnapshots, page.DBClusterSnapshots...)
			return true
		})
	if qerr != nil {
		fmt.Errorf("unable to retrieve rds instances with error: %v", qerr)
	}

	var entries [][]string = [][]string{
		{"snapshot_name", "instance_name", "creation_time", "snapshot_type", "storage_size_GiB"}}
	for _, snapshot := range dbsnapshots {
		entries = append(entries, []string{*snapshot.DBClusterSnapshotIdentifier, *snapshot.DBClusterIdentifier, snapshot.SnapshotCreateTime.String(), *snapshot.SnapshotType, strconv.Itoa(int(*snapshot.AllocatedStorage))})
		fmt.Printf("Snapshot Name: %s\n", *snapshot.DBClusterSnapshotIdentifier)
		fmt.Printf("Instance Name: %s\n", *snapshot.DBClusterIdentifier)
		fmt.Printf("Creation time: %s\n", *snapshot.SnapshotCreateTime)
		fmt.Printf("Snapshot type: %s\n", *snapshot.SnapshotType)
		fmt.Printf("Storage Size: %d GB\n", *snapshot.AllocatedStorage)
		fmt.Println("--------------")
	}
	writeCsv("RDSsnapshots.csv", entries)
}

func retrieveDDBTables(session session.Session) {
	client := dynamodb.New(&session)
	tables, _ := client.ListTables(&dynamodb.ListTablesInput{})
	totalSize := float64(0)

	var entries [][]string = [][]string{
		{"table_name", "table_size_GiB"}}
	for _, tableName := range tables.TableNames {
		tableSize, err := getTableSize(client, tableName)
		if err != nil {
			fmt.Printf("Error getting size for table %s: %v\n", *tableName, err)
			continue
		}
		entries = append(entries, []string{*tableName, fmt.Sprintf("%.2f", tableSize)})

		fmt.Printf("Table Name: %s\n", *tableName)
		fmt.Printf("Table Size: %s GiB\n", fmt.Sprintf("%.2f", tableSize))
		fmt.Println("--------------")
		totalSize += tableSize
	}
	writeCsv("DdbTables.csv", entries)
}

func getTableSize(svc *dynamodb.DynamoDB, tableName *string) (float64, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: tableName,
	}

	result, err := svc.DescribeTable(input)
	if err != nil {
		return 0, err
	}

	tableSize := *result.Table.TableSizeBytes
	return bytesToGiB(float64(tableSize)), nil
}

func bytesToGiB(bytes float64) float64 {
	gibibytes := bytes / math.Pow(2, 30)
	return gibibytes
}
