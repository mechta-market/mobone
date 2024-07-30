package model

import "time"

type Upsert struct {
	Id        int
	Name      string
	Test      bool
	Json      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (m *Upsert) CreateColumnMap() map[string]any {
	result := make(map[string]any, 5)

	result["name"] = m.Name
	result["test"] = m.Test
	result["json"] = m.Json
	result["created_at"] = m.CreatedAt
	result["updated_at"] = m.UpdatedAt

	return result
}

func (m *Upsert) UpdateColumnMap() map[string]any {
	return m.CreateColumnMap()
}

func (m *Upsert) ReturningColumnMap() map[string]any {
	return nil
}

func (m *Upsert) PKColumnMap() map[string]any {
	return map[string]any{
		"id": m.Id,
	}
}
