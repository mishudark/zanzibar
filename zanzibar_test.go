package zanzibar

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

var tuplesets = []string{
	"doc:readme#owner@10",
	"doc:readme#viewer@13",

	"group:devops#member@14",
	"group:all#member@group:devops#member",
	"doc:readme#viewer@group:all#member",

	"group:eng#member@11",
	"doc:readme#viewer@group:eng#member",

	"group:sales#member@12",
	"doc:readme#parent@folder:A#...",
	"folder:A#viewer@group:sales#member",

	"video:holmes#viewer@1",
	"group:eng#member@3",
	"video:holmes#parent@channel:audiobooks#...",
	"channel:audiobooks#editor@group:eng#member",
	"channel:audiobooks#viewer@2",
}

// key: object#relation
// value: user_id|userset
var records = map[string][]string{}

func init() {
	for _, tuple := range tuplesets {
		parts := strings.Split(tuple, "@")
		objectRelation := parts[0]
		user := parts[1]
		if _, ok := records[objectRelation]; !ok {
			records[objectRelation] = []string{user}
			continue
		}

		records[objectRelation] = append(records[objectRelation], user)
	}
}

type store struct {
	records map[string][]string
}

func (s *store) Save(tuple RelationTuple) error {
	return nil
}

func (s *store) Exact(userID string, object Object, relation string) error {
	errNotFound := errors.New("not found")

	key := fmt.Sprintf("%s:%s#%s", object.Namespace, object.ID, relation)
	recs := s.records[key]

	if len(recs) == 0 {
		return errNotFound
	}

	for _, rec := range recs {
		if rec == userID {
			return nil
		}
	}

	return errNotFound
}

func (s *store) Usersets(object Object, relation string) ([]Userset, error) {
	key := fmt.Sprintf("%s:%s#%s", object.Namespace, object.ID, relation)
	recs := s.records[key]

	var res []Userset
	for _, rec := range recs {
		parts := strings.Split(rec, "#")
		if len(parts) != 2 {
			continue
		}

		objectParts := strings.Split(parts[0], ":")

		res = append(res, Userset{
			Object: Object{
				Namespace: objectParts[0],
				ID:        objectParts[1],
			},
			Relation: parts[1],
		})
	}

	return res, nil
}

func TestCheck(t *testing.T) {
	inmemStore := &store{
		records: records,
	}

	auth := NewAuthorizationService(inmemStore, "testdata")

	tc := []struct {
		name     string
		expected bool
		given    string
	}{
		{
			name:     "not found relation on inexistent user",
			expected: false,
			given:    "doc:readme#viewer@15",
		},
		{
			name:     "not found relation",
			expected: false,
			given:    "doc:readme#viewer@10",
		},
		{
			name:     "not found relation",
			expected: false,
			given:    "doc:readme#viewer@12",
		},
		{
			name:     "relation by group found",
			expected: true,
			given:    "doc:readme#viewer@11",
		},

		{
			name:     "not found relation",
			expected: true,
			given:    "doc:readme#viewer@13",
		},
		{
			name:     "found by parent -> editor -> group -> member",
			expected: true,
			given:    "video:holmes#viewer@3",
		},
		{
			name:     "found by parent -> viewer",
			expected: true,
			given:    "video:holmes#viewer@2",
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {

			parts := strings.Split(tt.given, "@")
			objectRelation := parts[0]
			user := parts[1]

			objectRelationParts := strings.Split(objectRelation, "#")
			relation := objectRelationParts[1]

			objectParts := strings.Split(objectRelationParts[0], ":")
			namespace := objectParts[0]
			objectID := objectParts[1]

			object := Object{
				Namespace: namespace,
				ID:        objectID,
			}

			actual := auth.Check(user, object, relation)

			if actual != tt.expected {
				t.Errorf("given(%s): expected %t, actual %t", tt.given, tt.expected, actual)
			}
		})
	}
}
