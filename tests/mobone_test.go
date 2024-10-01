package tests

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/require"

	"github.com/mechta-market/mobone/v2"
	"github.com/mechta-market/mobone/v2/tests/model"
)

const tableName = "tests"

var dbCon *Con
var queryBuilder = squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)

func TestMain(m *testing.M) {
	dbName := os.Getenv("TEST_DB_NAME")
	if dbName == "" {
		dbName = "mobone"
	}

	err := recreateDB(dbName)
	if err != nil {
		log.Printf("recreateDB: %v\n", err)
		os.Exit(1)
	}

	dbCon, err = NewCon(dbName)
	if err != nil {
		log.Printf("NewCon: %v\n", err)
		os.Exit(1)
	}

	err = initSchema(dbCon)
	if err != nil {
		log.Printf("initSchema: %v\n", err)
		os.Exit(1)
	}

	// RUN TESTS
	exitCode := m.Run()

	dbCon.Close()

	os.Exit(exitCode)
}

func recreateDB(dbName string) error {
	ctx := context.Background()

	// recreate database
	{
		con, err := NewCon("")
		if err != nil {
			return fmt.Errorf("NewCon: %w", err)
		}
		defer con.Close()

		_, err = con.pool.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
		if err != nil {
			return fmt.Errorf("unable to drop database: %w", err)
		}

		_, err = con.pool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName))
		if err != nil {
			return fmt.Errorf("unable to create database: %w", err)
		}
	}

	return nil
}

func initSchema(con *Con) error {
	ctx := context.Background()

	_, err := con.pool.Exec(ctx, `
		CREATE TABLE `+tableName+` (
		    id SERIAL PRIMARY KEY,
		    created_at timestamptz not null default now(),
		    updated_at timestamptz not null default now(),
		    name text not null default '',
		    flag boolean not null default false,
		    contact jsonb not null default '{}'
		)
	`)
	if err != nil {
		return fmt.Errorf("unable to create table: %w", err)
	}

	return nil
}

func TestCreate(t *testing.T) {
	_, err := dbCon.pool.Exec(context.Background(), "TRUNCATE TABLE "+tableName+" RESTART IDENTITY")
	require.NoError(t, err)

	ctx := context.Background()

	modelStore := mobone.ModelStore{
		Con:       dbCon.pool,
		QB:        queryBuilder,
		TableName: tableName,
	}

	item := &model.Select{
		Name: "Test Model",
		Flag: true,
		Contact: model.Contact{
			Phone: "123456789",
			Email: "test@example.com",
		},
	}

	createModel := &model.Upsert{
		Name: &item.Name,
		Flag: &item.Flag,
		Contact: &model.ContactEdit{
			Phone: &item.Contact.Phone,
			Email: &item.Contact.Email,
		},
	}
	err = modelStore.Create(ctx, createModel)
	require.NoError(t, err)
	require.Greater(t, createModel.PKId, 0)
	item.Id = createModel.PKId

	dbItem := &model.Select{Id: item.Id}
	found, err := modelStore.Get(ctx, dbItem)
	require.NoError(t, err)
	require.True(t, found)
	require.WithinDuration(t, time.Now(), dbItem.CreatedAt, 30*time.Millisecond)
	require.WithinDuration(t, time.Now(), dbItem.UpdatedAt, 30*time.Millisecond)
	dbItem.CreatedAt = time.Time{}
	dbItem.UpdatedAt = time.Time{}
	require.Equal(t, item, dbItem)

	dbItems := make([]*model.Select, 0, 3)
	_, err = modelStore.List(ctx, mobone.ListParams{
		PageSize: 10,
	}, func(add bool) mobone.ListModelI {
		x := &model.Select{}
		if add {
			dbItems = append(dbItems, x)
		}
		return x
	})
	require.NoError(t, err)
	require.Len(t, dbItems, 1)
	dbItem = dbItems[0]
	dbItem.CreatedAt = time.Time{}
	dbItem.UpdatedAt = time.Time{}
	require.Equal(t, item, dbItem)
}

func TestUpdate(t *testing.T) {
	_, err := dbCon.pool.Exec(context.Background(), "TRUNCATE TABLE "+tableName+" RESTART IDENTITY")
	require.NoError(t, err)

	ctx := context.Background()

	modelStore := mobone.ModelStore{
		Con:       dbCon.pool,
		QB:        queryBuilder,
		TableName: tableName,
	}

	item := &model.Select{
		Name: "Test Model",
		Flag: true,
		Contact: model.Contact{
			Phone: "123456789",
			Email: "test@example.com",
		},
	}

	createModel := &model.Upsert{
		Name: &item.Name,
		Flag: &item.Flag,
		Contact: &model.ContactEdit{
			Phone: &item.Contact.Phone,
			Email: &item.Contact.Email,
		},
	}
	err = modelStore.Create(ctx, createModel)
	require.NoError(t, err)
	item.Id = createModel.PKId

	item.UpdatedAt = time.Now().Add(-time.Hour)
	item.Name = "Test Model changed"
	item.Flag = false
	item.Contact.Phone = "987654321"
	item.Contact.Email = "changed@example.com"

	updateModel := &model.Upsert{
		PKId:      item.Id,
		UpdatedAt: &item.UpdatedAt,
		Name:      &item.Name,
		Flag:      &item.Flag,
		Contact: &model.ContactEdit{
			Phone: &item.Contact.Phone,
			Email: &item.Contact.Email,
		},
	}
	err = modelStore.Update(ctx, updateModel)
	require.NoError(t, err)
	require.Greater(t, updateModel.PKId, 0)
	item.Id = updateModel.PKId

	dbItem := &model.Select{Id: item.Id}
	found, err := modelStore.Get(ctx, dbItem)
	require.NoError(t, err)
	require.True(t, found)
	require.WithinDuration(t, time.Now(), dbItem.CreatedAt, 30*time.Millisecond)
	require.WithinDuration(t, item.UpdatedAt, dbItem.UpdatedAt, 30*time.Millisecond)
	dbItem.CreatedAt = time.Time{}
	dbItem.UpdatedAt = item.UpdatedAt
	require.Equal(t, item, dbItem)
}

func TestList(t *testing.T) {
	_, err := dbCon.pool.Exec(context.Background(), "TRUNCATE TABLE "+tableName+" RESTART IDENTITY")
	require.NoError(t, err)

	ctx := context.Background()

	modelStore := mobone.ModelStore{
		Con:       dbCon.pool,
		QB:        queryBuilder,
		TableName: tableName,
	}

	item := &model.Select{
		Name: "Test Model",
	}

	createModel := &model.Upsert{
		Name: &item.Name,
	}
	err = modelStore.Create(ctx, createModel)
	require.NoError(t, err)
	item.Id = createModel.PKId

	item2 := &model.Select{
		Name: "Test Model 2",
	}

	createModel = &model.Upsert{
		Name: &item2.Name,
	}
	err = modelStore.Create(ctx, createModel)
	require.NoError(t, err)
	item2.Id = createModel.PKId

	dbItems := make([]*model.Select, 0, 3)
	_, err = modelStore.List(ctx, mobone.ListParams{
		PageSize: 10,
		Sort:     []string{"id"},
	}, func(add bool) mobone.ListModelI {
		x := &model.Select{}
		if add {
			dbItems = append(dbItems, x)
		}
		return x
	})
	require.NoError(t, err)
	require.Len(t, dbItems, 2)
	dbItems[0].CreatedAt = time.Time{}
	dbItems[0].UpdatedAt = time.Time{}
	dbItems[1].CreatedAt = time.Time{}
	dbItems[1].UpdatedAt = time.Time{}
	require.Equal(t, item, dbItems[0])
	require.Equal(t, item2, dbItems[1])
}

func TestListWithOnlyCount(t *testing.T) {
	_, err := dbCon.pool.Exec(context.Background(), "TRUNCATE TABLE "+tableName+" RESTART IDENTITY")
	require.NoError(t, err)

	ctx := context.Background()

	modelStore := mobone.ModelStore{
		Con:       dbCon.pool,
		QB:        queryBuilder,
		TableName: tableName,
	}

	item := &model.Select{
		Name: "Test Model",
	}

	createModel := &model.Upsert{
		Name: &item.Name,
	}
	err = modelStore.Create(ctx, createModel)
	require.NoError(t, err)
	item.Id = createModel.PKId

	listCount, err := modelStore.List(ctx, mobone.ListParams{
		OnlyCount: true,
	}, func(add bool) mobone.ListModelI {
		return &model.Select{}
	})
	require.NoError(t, err)
	require.Equal(t, 1, int(listCount))
}

func TestDelete(t *testing.T) {
	_, err := dbCon.pool.Exec(context.Background(), "TRUNCATE TABLE "+tableName+" RESTART IDENTITY")
	require.NoError(t, err)

	ctx := context.Background()

	modelStore := mobone.ModelStore{
		Con:       dbCon.pool,
		QB:        queryBuilder,
		TableName: tableName,
	}

	item := &model.Select{
		Name: "Test Model",
	}

	createModel := &model.Upsert{
		Name: &item.Name,
	}
	err = modelStore.Create(ctx, createModel)
	require.NoError(t, err)
	item.Id = createModel.PKId

	deleteModel := &model.Upsert{PKId: item.Id}
	err = modelStore.Delete(ctx, deleteModel)
	require.NoError(t, err)

	listCount, err := modelStore.List(ctx, mobone.ListParams{
		OnlyCount: true,
	}, func(add bool) mobone.ListModelI {
		return &model.Select{}
	})
	require.NoError(t, err)
	require.Equal(t, 0, int(listCount))
}

func TestJsonMerge(t *testing.T) {
	_, err := dbCon.pool.Exec(context.Background(), "TRUNCATE TABLE "+tableName+" RESTART IDENTITY")
	require.NoError(t, err)

	ctx := context.Background()

	modelStore := mobone.ModelStore{
		Con:       dbCon.pool,
		QB:        queryBuilder,
		TableName: tableName,
	}

	item := &model.Select{
		Name: "Name",
		Contact: model.Contact{
			Email: "test@example.com",
		},
	}

	createModel := &model.Upsert{
		Name: &item.Name,
		Contact: &model.ContactEdit{
			Email: &item.Contact.Email,
		},
	}
	err = modelStore.Create(ctx, createModel)
	require.NoError(t, err)
	item.Id = createModel.PKId

	dbItem := &model.Select{Id: item.Id}
	_, err = modelStore.Get(ctx, dbItem)
	require.NoError(t, err)
	dbItem.CreatedAt = time.Time{}
	dbItem.UpdatedAt = time.Time{}
	require.Equal(t, item, dbItem)

	item.Contact.Phone = "123456789"

	err = modelStore.Update(ctx, &model.Upsert{
		PKId: item.Id,
		Contact: &model.ContactEdit{
			Phone: &item.Contact.Phone,
		},
	})
	require.NoError(t, err)

	dbItem = &model.Select{Id: item.Id}
	_, err = modelStore.Get(ctx, dbItem)
	require.NoError(t, err)
	dbItem.CreatedAt = time.Time{}
	dbItem.UpdatedAt = time.Time{}
	require.Equal(t, item, dbItem)

	item.Contact.Email = "changed@example.com"

	err = modelStore.Update(ctx, &model.Upsert{
		PKId: item.Id,
		Contact: &model.ContactEdit{
			Email: &item.Contact.Email,
		},
	})
	require.NoError(t, err)

	dbItem = &model.Select{Id: item.Id}
	_, err = modelStore.Get(ctx, dbItem)
	require.NoError(t, err)
	dbItem.CreatedAt = time.Time{}
	dbItem.UpdatedAt = time.Time{}
	require.Equal(t, item, dbItem)
}
