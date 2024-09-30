package model

import "time"

type Upsert struct {
	PKId int

	UpdatedAt *time.Time
	Name      *string
	Flag      *bool
	Contact   *Contact
}

func (m *Upsert) CreateColumnMap() map[string]any {
	result := make(map[string]any, 5)

	if m.UpdatedAt != nil {
		result["updated_at"] = *m.UpdatedAt
	}

	if m.Name != nil {
		result["name"] = *m.Name
	}

	if m.Flag != nil {
		result["flag"] = *m.Flag
	}

	if m.Contact != nil {
		result["contact"] = m.Contact
	}

	return result
}

func (m *Upsert) UpdateColumnMap() map[string]any {
	result := m.CreateColumnMap()
	for k := range m.PKColumnMap() {
		delete(result, k)
	}
	return result
}

func (m *Upsert) ReturningColumnMap() map[string]any {
	return map[string]any{
		"id": &m.PKId,
	}
}

func (m *Upsert) PKColumnMap() map[string]any {
	return map[string]any{
		"id": m.PKId,
	}
}
