package zanzibar

import (
	"fmt"
	"log"
	"strings"

	"github.com/dgraph-io/badger"
	"github.com/mishudark/zanzibar"
)

const objRelationTpl = "%s:%s#%s"

type tupleStore struct {
	db *badger.DB
}

func (t *tupleStore) Exact(userID string, object zanzibar.Object, relation string) error {
	err := t.db.View(func(txn *badger.Txn) error {
		key := fmt.Sprintf(objRelationTpl+"@%s", object.Namespace, object.ID, relation, userID)
		_, err := txn.Get([]byte(key))
		return err
	})

	return err
}

func (t *tupleStore) Usersets(object zanzibar.Object, relation string) ([]zanzibar.Userset, error) {
	err := t.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		prefixStr := fmt.Sprintf(objRelationTpl+"@", object.Namespace, object.ID, relation)
		prefix := []byte(prefixStr)

		var res []zanzibar.Userset
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := string(item.Key())

			rec := k[strings.LastIndex(k, "@")+1:]

			parts := strings.Split(rec, "#")
			if len(parts) != 2 {
				continue
			}

			objectParts := strings.Split(parts[0], ":")

			res = append(res, zanzibar.Userset{
				Object: zanzibar.Object{
					Namespace: objectParts[0],
					ID:        objectParts[1],
				},
				Relation: parts[1],
			})
		}
		return nil
	})

	return nil, err
}

func (t *tupleStore) Save(tuple zanzibar.RelationTuple) error {
	str := fmt.Sprintf(objRelationTpl, tuple.Object.Namespace, tuple.Object.ID, tuple.Relation)

	if tuple.User.UserID == "" {
		userset := tuple.User.Userset
		userStr := fmt.Sprintf(objRelationTpl, userset.Object.Namespace, userset.Object.ID, userset.Relation)
		str += "@" + userStr
	} else {
		str += "@" + tuple.User.UserID
	}

	err := t.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(str), []byte{0})
	})
	return err
}

// OpenDB using the given path
func OpenDB(path string) *badger.DB {
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	return db
}

// NewTupleStore handles store methods using badger as a key value storage engine
func NewTupleStore(db *badger.DB) zanzibar.TupleStore {
	return &tupleStore{
		db: db,
	}
}
