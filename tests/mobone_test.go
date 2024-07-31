package tests

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	"github.com/mechta-market/mobone/v2"
	"github.com/mechta-market/mobone/v2/tests/model"
)

var pgxPool *pgxpool.Pool
var queryBuilder squirrel.StatementBuilderType
var dbName = "mobone"

func TestMain(m *testing.M) {
	code := 0
	err := initDB()

	defer pgxPool.Close()

	if err == nil {
		code = m.Run()
	}

	os.Exit(code)
}

func initDB() error {
	connDsn := os.Getenv("DATABASE_URL")
	if connDsn == "" {
		connDsn = "postgres://default:passw0rd@postgres_test:5432"
	}

	var err error
	pgxPool, err = pgxpool.New(context.Background(), connDsn+"/postgres")

	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}

	queryBuilder = squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)

	_, err = pgxPool.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))

	if err != nil {
		log.Printf("Unable to drop database: %v\n", err)
		return err
	}
	log.Printf("Database %s dropped\n", dbName)

	_, err = pgxPool.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE %s", dbName))
	if err != nil {
		log.Printf("Unable to create database: %v\n", err)
		return err
	}
	log.Printf("Database %s created\n", dbName)
	pgxPool.Close()

	pgxPool, err = pgxpool.New(context.Background(), connDsn+"/"+dbName)
	if err != nil {
		log.Printf("Unable to connect to database: %v\n", err)
	}

	createTable := "CREATE TABLE tests (id SERIAL PRIMARY KEY, name varchar(255), test boolean, json json, created_at timestamp, updated_at timestamp)"

	_, err = pgxPool.Exec(context.Background(), createTable)
	if err != nil {
		log.Printf("Unable to create table: %v\n", err)
		return err
	}

	log.Print("Table tests created\n")

	return nil
}

func TestCreate(t *testing.T) {
	upsertModel := &model.Upsert{
		Name:      "Test Model",
		Test:      true,
		Json:      `{"test": true}`,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	modelStore := mobone.ModelStore{pgxPool, queryBuilder, "tests"}

	err := modelStore.Create(context.Background(), upsertModel)

	assert.NoErrorf(t, err, "ModelStore.Get: %w", err)

	m := &model.Select{Id: 1}
	found, err := modelStore.Get(context.Background(), m)
	assert.NoErrorf(t, err, "ModelStore.Get: %w", err)
	assert.True(t, found)
	assert.Equal(t, true, m.Test)
}

func TestUpdate(t *testing.T) {
	upsertModel := &model.Upsert{
		Id:   1,
		Name: "Test Model",
		Test: false,
		Json: `{"test": false}`,
	}

	modelStore := mobone.ModelStore{pgxPool, queryBuilder, "tests"}

	err := modelStore.Update(context.Background(), upsertModel)

	assert.NoErrorf(t, err, "ModelStore.Get: %w", err)

	m := &model.Select{Id: 1}
	found, err := modelStore.Get(context.Background(), m)
	assert.NoErrorf(t, err, "ModelStore.Get: %w", err)
	assert.True(t, found)
	assert.Equal(t, false, m.Test)
}

func TestGet(t *testing.T) {
	m := &model.Select{
		Id: 1,
	}

	modelStore := mobone.ModelStore{pgxPool, queryBuilder, "tests"}

	found, err := modelStore.Get(context.Background(), m)

	assert.NoErrorf(t, err, "ModelStore.Get: %w", err)
	assert.True(t, found)
	assert.Equal(t, "Test Model", m.Name)
}

func TestList(t *testing.T) {
	conditions := map[string]any{
		"Name": "Test Model",
	}
	conditionExps := map[string][]any{}

	items := make([]*model.Select, 0)

	modelStore := mobone.ModelStore{pgxPool, queryBuilder, "tests"}

	totalCount, err := modelStore.List(context.Background(), mobone.ListParams{
		Conditions:           conditions,
		ConditionExpressions: conditionExps,
		Page:                 0,
		PageSize:             5,
		WithTotalCount:       false,
		OnlyCount:            false,
		Sort:                 []string{"id"},
	}, func(add bool) mobone.ListModelI {
		item := &model.Select{}
		if add {
			items = append(items, item)
		}
		return item
	})

	assert.NoErrorf(t, err, "ModelStore.List: %w", err)
	assert.Equal(t, 1, len(items))
	assert.Equal(t, int64(0), totalCount)
	assert.Equal(t, "Test Model", items[0].Name)
}

func TestListWithTotalCount(t *testing.T) {
	conditions := map[string]any{
		"Name": "Test Model",
	}
	conditionExps := map[string][]any{}

	items := make([]*model.Select, 0)

	modelStore := mobone.ModelStore{pgxPool, queryBuilder, "tests"}

	totalCount, err := modelStore.List(context.Background(), mobone.ListParams{
		Conditions:           conditions,
		ConditionExpressions: conditionExps,
		Page:                 0,
		PageSize:             5,
		WithTotalCount:       true,
		OnlyCount:            false,
		Sort:                 []string{"id"},
	}, func(add bool) mobone.ListModelI {
		item := &model.Select{}
		if add {
			items = append(items, item)
		}
		return item
	})

	assert.NoErrorf(t, err, "ModelStore.List: %w", err)
	assert.Equal(t, 1, len(items))
	assert.Equal(t, int64(1), totalCount)
	assert.Equal(t, "Test Model", items[0].Name)
}

func TestListWithOnlyCount(t *testing.T) {
	conditions := map[string]any{
		"Name": "Test Model",
	}
	conditionExps := map[string][]any{}

	items := make([]*model.Select, 0)

	modelStore := mobone.ModelStore{pgxPool, queryBuilder, "tests"}

	totalCount, err := modelStore.List(context.Background(), mobone.ListParams{
		Conditions:           conditions,
		ConditionExpressions: conditionExps,
		Page:                 0,
		PageSize:             5,
		WithTotalCount:       true,
		OnlyCount:            true,
		Sort:                 []string{"id"},
	}, func(add bool) mobone.ListModelI {
		item := &model.Select{}
		if add {
			items = append(items, item)
		}
		return item
	})

	assert.NoErrorf(t, err, "ModelStore.List: %w", err)
	assert.Equal(t, 0, len(items))
	assert.Equal(t, int64(1), totalCount)
}

func TestDelete(t *testing.T) {
	modelStore := mobone.ModelStore{pgxPool, queryBuilder, "tests"}

	deleteModel := &model.Upsert{
		Id: 1,
	}

	err := modelStore.Delete(context.Background(), deleteModel)
	assert.NoErrorf(t, err, "ModelStore.Delete: %w", err)

	m := &model.Select{Id: 1}
	found, err := modelStore.Get(context.Background(), m)
	assert.False(t, found)
}
