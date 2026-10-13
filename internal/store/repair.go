package store

import "context"

// ExistingMessageIDs lets reconciliation insert missing history without replacing
// cached attachment state or newer local snapshots for already stored messages.
func (s *Store) ExistingMessageIDs(ctx context.Context, first, last int64) (map[int64]bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM messages WHERE id BETWEEN ? AND ?`, first, last)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids[id] = true
	}
	return ids, rows.Err()
}
