package zanzibar

import (
	"io/ioutil"
	"testing"

	"github.com/dgraph-io/badger"
	"github.com/mishudark/zanzibar"
)

func TestExact(t *testing.T) {
	path, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}

	db := OpenDB(path)
	store := NewTupleStore(db)

	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("doc:readme#viewer@1"), []byte{0})
	})

	if err != nil {
		t.Fatal(err)
		return
	}

	var tests = []struct {
		name     string
		object   zanzibar.Object
		relation string
		userID   string
		hasErr   bool
	}{
		{
			name: "exact match",
			object: zanzibar.Object{
				Namespace: "doc",
				ID:        "readme",
			},
			relation: "viewer",
			userID:   "1",
			hasErr:   false,
		},
		{
			name: "exact match",
			object: zanzibar.Object{
				Namespace: "doc",
				ID:        "readme",
			},
			relation: "viewer",
			userID:   "2",
			hasErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.Exact(tt.userID, tt.object, tt.relation)
			actual := (err != nil) != tt.hasErr
			if actual {
				t.Errorf("expected %t, actual %t", tt.hasErr, actual)
				return
			}
		})
	}
}

func TestSave(t *testing.T) {
	path, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}

	db := OpenDB(path)
	store := NewTupleStore(db)

	var tests = []struct {
		name  string
		given zanzibar.RelationTuple
		using string
	}{
		{
			name:  "tuple with user id",
			using: "doc:readme#viewer@1",
			given: zanzibar.RelationTuple{
				Object: zanzibar.Object{
					Namespace: "doc",
					ID:        "readme",
				},
				Relation: "viewer",
				User: zanzibar.User{
					Userset: zanzibar.Userset{
						Relation: "",
						Object: zanzibar.Object{
							Namespace: "",
							ID:        "",
						},
					},
					UserID: "1",
				},
			},
		},
		{
			name:  "tuple with userset",
			using: "doc:readme#viewer@group:eng#member",
			given: zanzibar.RelationTuple{
				Object: zanzibar.Object{
					Namespace: "doc",
					ID:        "readme",
				},
				Relation: "viewer",
				User: zanzibar.User{
					Userset: zanzibar.Userset{
						Relation: "member",
						Object: zanzibar.Object{
							Namespace: "group",
							ID:        "eng",
						},
					},
					UserID: "",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.Save(tt.given)
			if err != nil {
				t.Error(err)
				return
			}

			err = db.View(func(txn *badger.Txn) error {
				_, err := txn.Get([]byte(tt.using))
				return err
			})

			if err != nil {
				t.Error(err)
			}
		})
	}
}
