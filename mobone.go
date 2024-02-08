package mobone

import (
	"context"
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ListModelI interface {
	ListColumnMap() map[string]any
	DefaultSortColumns() []string
}

type GetModelI interface {
	ListColumnMap() map[string]any
	PKColumnMap() map[string]any
}

type CreateModelI interface {
	CreateColumnMap() map[string]any
	ReturningColumnMap() map[string]any
}

type UpdateModelI interface {
	UpdateColumnMap() map[string]any
	PKColumnMap() map[string]any
}

type ListParams struct {
	Conditions           map[string]any
	ConditionExpressions map[string][]any
	Distinct             bool
	Columns              []string
	Page                 int64
	PageSize             int64
	WithTotalCount       bool
	OnlyCount            bool
	Sort                 []string
}

type ModelStore[ListModel ListModelI, GetModel GetModelI, CreateModel CreateModelI, UpdateModel UpdateModelI] struct {
	Con                  *pgxpool.Pool
	QB                   squirrel.StatementBuilderType
	TableName            string
	ListModelConstructor func() ListModel
}

func (s *ModelStore[ListModel, GetModel, CreateModel, UpdateModel]) Create(ctx context.Context, m CreateModel) error {
	queryBuilder := s.QB.Insert(s.TableName).
		SetMap(m.CreateColumnMap())

	returningColumnMap := m.ReturningColumnMap()
	returningColumnNames := make([]string, 0, len(returningColumnMap))
	returningFieldPointers := make([]any, 0, len(returningColumnMap))
	for k, v := range returningColumnMap {
		returningColumnNames = append(returningColumnNames, k)
		returningFieldPointers = append(returningFieldPointers, v)
	}

	if len(returningColumnNames) > 0 {
		queryBuilder = queryBuilder.Suffix(`RETURNING ` + strings.Join(returningColumnNames, ","))
	}

	query, args, err := queryBuilder.ToSql()
	if err != nil {
		return fmt.Errorf("fail to build query: %w", err)
	}

	if len(returningColumnNames) > 0 {
		var rows pgx.Rows

		rows, err = s.Con.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("fail to query: %w", err)
		}
		defer rows.Close()

		if !rows.Next() {
			if err = rows.Err(); err != nil {
				return fmt.Errorf("rows.Err: %w", err)
			}
			return nil
		}

		err = rows.Scan(returningFieldPointers...)
		if err != nil {
			return fmt.Errorf("fail to scan: %w", err)
		}
	} else {
		_, err = s.Con.Exec(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("fail to exec: %w", err)
		}
	}

	return nil
}

func (s *ModelStore[ListModel, GetModel, CreateModel, UpdateModel]) Update(ctx context.Context, m UpdateModel) error {
	queryBuilder := s.QB.Update(s.TableName).
		SetMap(m.UpdateColumnMap())

	for k, v := range m.PKColumnMap() {
		queryBuilder = queryBuilder.Where(k+` = ?`, v)
	}

	query, args, err := queryBuilder.ToSql()
	if err != nil {
		return fmt.Errorf("fail to build query: %w", err)
	}

	//fmt.Println(query, args)

	_, err = s.Con.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("fail to exec: %w", err)
	}

	return nil
}

func (s *ModelStore[ListModel, GetModel, CreateModel, UpdateModel]) Delete(ctx context.Context, m UpdateModel) error {
	queryBuilder := s.QB.Delete(s.TableName)

	for k, v := range m.PKColumnMap() {
		queryBuilder = queryBuilder.Where(k+` = ?`, v)
	}

	query, args, err := queryBuilder.ToSql()
	if err != nil {
		return fmt.Errorf("fail to build query: %w", err)
	}

	_, err = s.Con.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("fail to exec: %w", err)
	}

	return nil
}

func (s *ModelStore[ListModel, GetModel, CreateModel, UpdateModel]) List(ctx context.Context, params ListParams) ([]ListModel, int64, error) {
	queryBuilder := s.QB.Select().From(s.TableName)

	// conditions
	if params.Conditions != nil {
		queryBuilder = queryBuilder.Where(params.Conditions)
	}
	if params.ConditionExpressions != nil {
		for expression, args := range params.ConditionExpressions {
			queryBuilder = queryBuilder.Where(expression, args...)
		}
	}

	var totalCount int64

	listItemInstance := s.ListModelConstructor()

	// construct column names
	allowedColMap := listItemInstance.ListColumnMap()
	colNames := make([]string, 0, len(params.Columns))
	if len(params.Columns) > 0 {
		var ok bool
		for _, colName := range params.Columns {
			if _, ok = allowedColMap[colName]; ok {
				colNames = append(colNames, colName)
			}
		}
	} else {
		for colName := range allowedColMap {
			colNames = append(colNames, colName)
		}
	}
	if len(colNames) == 0 {
		return nil, 0, fmt.Errorf("no columns")
	}

	// total count
	if params.WithTotalCount || params.OnlyCount {
		if params.Distinct {
			queryBuilder = queryBuilder.Column(`count(distinct (` + strings.Join(colNames, ",") + `))`)
		} else {
			queryBuilder = queryBuilder.Column(`count(*)`)
		}

		query, args, err := queryBuilder.ToSql()
		if err != nil {
			return nil, 0, fmt.Errorf("fail to build query: %w", err)
		}

		rows, err := s.Con.Query(ctx, query, args...)
		if err != nil {
			return nil, 0, fmt.Errorf("fail to query: %w", err)
		}
		defer rows.Close()

		if !rows.Next() {
			if err = rows.Err(); err != nil {
				return nil, 0, fmt.Errorf("rows.Err: %w", err)
			}
			return nil, 0, fmt.Errorf("no rows for 'select count(*)' query")
		}

		err = rows.Scan(&totalCount)
		if err != nil {
			return nil, 0, fmt.Errorf("fail to scan: %w", err)
		}

		if params.OnlyCount {
			return nil, totalCount, nil
		}

		queryBuilder = queryBuilder.RemoveColumns()
	}

	// apply columns
	if params.Distinct {
		queryBuilder = queryBuilder.Distinct()
	}
	queryBuilder = queryBuilder.Columns(colNames...)

	// pagination
	if params.PageSize > 0 {
		queryBuilder = queryBuilder.Offset(uint64(params.Page * params.PageSize)).Limit(uint64(params.PageSize))
	}

	// sort
	if params.Sort == nil {
		sortColumns := listItemInstance.DefaultSortColumns()
		if len(sortColumns) > 0 {
			queryBuilder = queryBuilder.OrderBy(sortColumns...)
		}
	} else if len(params.Sort) > 0 {
		queryBuilder = queryBuilder.OrderBy(params.Sort...)
	}

	// build query
	query, args, err := queryBuilder.ToSql()
	if err != nil {
		return nil, 0, fmt.Errorf("fail to build query: %w", err)
	}

	//slog.Info("List query", "query", query, "args", args)

	// execute query
	rows, err := s.Con.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("fail to query: %w", err)
	}
	defer rows.Close()

	mm := make([]ListModel, 0)

	for rows.Next() {
		m := s.ListModelConstructor()

		err = rows.Scan(fieldPointersForColNames(m, colNames)...)
		if err != nil {
			return nil, 0, fmt.Errorf("fail to scan: %w", err)
		}

		mm = append(mm, m)
	}
	if err = rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("rows.Err: %w", err)
	}

	return mm, totalCount, nil
}

func (s *ModelStore[ListModel, GetModel, CreateModel, UpdateModel]) Get(ctx context.Context, m GetModelI) (bool, error) {
	colMap := m.ListColumnMap()
	colNames := make([]string, 0, len(colMap))
	colFieldPointers := make([]any, 0, len(colMap))
	for colName, fieldPointer := range colMap {
		colNames = append(colNames, colName)
		colFieldPointers = append(colFieldPointers, fieldPointer)
	}

	if len(colNames) == 0 {
		return false, fmt.Errorf("no columns")
	}

	queryBuilder := s.QB.Select(colNames...).
		From(s.TableName).
		Limit(1)

	for k, v := range m.PKColumnMap() {
		queryBuilder = queryBuilder.Where(k+` = ?`, v)
	}

	query, args, err := queryBuilder.ToSql()
	if err != nil {
		return false, fmt.Errorf("fail to build query: %w", err)
	}

	rows, err := s.Con.Query(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("fail to query: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err = rows.Err(); err != nil {
			return false, fmt.Errorf("rows.Err: %w", err)
		}
		return false, nil
	}

	err = rows.Scan(colFieldPointers...)
	if err != nil {
		return false, fmt.Errorf("fail to scan: %w", err)
	}

	return true, nil
}

func fieldPointersForColNames(m ListModelI, colNames []string) []any {
	colMap := m.ListColumnMap()
	result := make([]any, 0, len(colNames))
	for _, colName := range colNames {
		result = append(result, colMap[colName])
	}
	return result
}
