package zanzibar

import (
	"fmt"
	"log"
	"strings"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/mishudark/zanzibar"
)

const (
	objRelationTpl = "%s:%s#%s"
	// userSetMark is added to user part to detect when it is not an user_id, it is useful to retrieve
	// just usersets when they are needed intad the whole usersets + user_ids
	// user_id is represented in the tuple as
	//		doc:readme#viewer@2
	// userset is represented in the tuple as
	//		doc:readme#viewer@|group:eng#member
	userSetMark = "|"
)

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
	var res []zanzibar.Userset
	err := t.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		prefixStr := fmt.Sprintf(objRelationTpl+"@"+userSetMark, object.Namespace, object.ID, relation)
		prefix := []byte(prefixStr)

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := string(item.Key())
			rec := k[strings.LastIndex(k, "@"+userSetMark)+2:] // it is +2 due to the lenght of the separator @|

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

	return res, err
}

func (t *tupleStore) Save(tuple zanzibar.RelationTuple) error {
	str := fmt.Sprintf(objRelationTpl, tuple.Object.Namespace, tuple.Object.ID, tuple.Relation)

	if tuple.User.UserID == "" {
		userset := tuple.User.Userset
		usersetStr := fmt.Sprintf(objRelationTpl, userset.Object.Namespace, userset.Object.ID, userset.Relation)
		str += "@" + userSetMark + usersetStr
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
	opts := badger.DefaultOptions(path)
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
