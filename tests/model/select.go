package model

import "time"

type Select struct {
	Id        int
	Name      string
	Test      bool
	Json      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (m *Select) ListColumnMap() map[string]any {
	return map[string]any{
		"id":         &m.Id,
		"name":       &m.Name,
		"test":       &m.Test,
		"json":       &m.Json,
		"created_at": &m.CreatedAt,
		"updated_at": &m.UpdatedAt,
	}
}

func (m *Select) PKColumnMap() map[string]any {
	return map[string]any{
		"id": m.Id,
	}
}

func (m *Select) DefaultSortColumns() []string {
	return []string{"id"}
}
