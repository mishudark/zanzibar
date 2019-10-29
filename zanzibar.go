package zanzibar

// Object representas a namespace and id in the form of `namespace:object_id`
type Object struct {
	Namespace string
	ID        string
}

// Userset represents an object and relation in the form of `object#relation`
type Userset struct {
	Relation string
	Object   Object
}

// User can be either an Userset or an UserID
type User struct {
	Userset Userset
	UserID  string
}

// RelationTuple is a relation between an user and an object
// `group:eng#member@11``
type RelationTuple struct {
	Object   Object
	Relation string
	User     User
}

// RelationParent is used to inherit relations
const RelationParent = "parent"

// TupleStore represents access to a data store
type TupleStore interface {
	Exact(userID string, object Object, relation string) error
	Usersets(object Object, relation string) ([]Userset, error)
	Save(tuple RelationTuple) error
}

type UsersetRewrite struct {
	Union []struct {
		ComputedUserset struct {
			Relation string `yaml:"relation"`
		} `yaml:"computed_userset,omitempty"`
		TupleToUserset struct {
			Tupleset struct {
				Relation string `yaml:"relation"`
			} `yaml:"tupleset"`
			ComputedUserset struct {
				Object   string `yaml:"object"`
				Relation string `yaml:"relation"`
			} `yaml:"computed_userset"`
		} `yaml:"tuple_to_userset,omitempty"`
	} `yaml:"union"`
}

type NamespaceConfig struct {
	Name      string `yaml:"name"`
	Relations []struct {
		Name           string         `yaml:"name"`
		UsersetRewrite UsersetRewrite `yaml:"userset_rewrite,omitempty"`
	} `yaml:"relations"`
}

// Authorization ...
type Authorization struct {
	store TupleStore
	// key: namespace#relation, value: UsersetRewrite
	// key: doc#owner
	computedRules map[string]UsersetRewrite
}

func (a *Authorization) computedUserset(object Object, relation string) []Userset {
	rules, ok := a.computedRules[object.Namespace+"#"+relation]
	if !ok || len(rules.Union) == 0 {
		return []Userset{}
	}

	var res []Userset
	for _, alias := range rules.Union {
		aliasRelation := alias.ComputedUserset.Relation
		if aliasRelation == "" {
			continue
		}

		res = append(res, Userset{
			Object:   object,
			Relation: aliasRelation,
		})
	}

	return res
}

func (a *Authorization) tupleUserset(object Object, relation string) []Userset {
	rules, ok := a.computedRules[object.Namespace+"#"+relation]
	if !ok || len(rules.Union) == 0 {
		return []Userset{}
	}

	var res []Userset
	for _, alias := range rules.Union {
		tuplesetRelation := alias.TupleToUserset.Tupleset.Relation
		if tuplesetRelation == "" {
			continue
		}

		usersets, err := a.store.Usersets(object, tuplesetRelation)
		if err != nil {
			continue
		}

		aliasRelation := alias.TupleToUserset.ComputedUserset.Relation
		if aliasRelation == "" {
			continue
		}

		// filter by aliasRelation
		// if the userset doesn't have a relation(represented by `...`), asign aliasRelation, sample:
		//		`channel:audiobooks#...`
		//  transform to:
		//    `channel:audiobooks#editor`
		for i := range usersets {
			rel := usersets[i].Relation
			if rel == "..." {
				usersets[i].Relation = aliasRelation
			}

			if usersets[i].Relation == aliasRelation {
				res = append(res, usersets[i])
			}
		}
	}

	return res
}

// Solution
// 1) check for directly match: object#relation@user_id
// 2) get all the userSets for the given object#relation
// 2.1) call CHECK api again using the previous userSets and the initial user_id
// 3) apply transformations based on namespace config
// 3.1) get all the userSets that match: userSetObject#relation
// 3.2) call CHECK api again using the previous userSets and the initial user_id

// Check in the form of "does user U have relation R to object O?"
func (a *Authorization) Check(userID string, object Object, relation string) bool {
	err := a.store.Exact(userID, object, relation)
	if err == nil {
		return true
	}

	// get usersets related to actual query
	usersets, err := a.store.Usersets(object, relation)
	if err != nil {
		return false
	}

	// apply transformations based on namespace config
	// TODO: we should use the userset namespace
	usersets = append(usersets, a.computedUserset(object, relation)...)
	usersets = append(usersets, a.tupleUserset(object, relation)...)

	for _, set := range usersets {
		if a.Check(userID, set.Object, set.Relation) {
			return true
		}
	}

	return false
}

// NewAuthorizationService returns an Authorization service using the given store, and reads from
// the given directory yaml files containing namespace configs
func NewAuthorizationService(store TupleStore, rewriteRulesPath string) *Authorization {
	rules, err := LoadRewriteRules(rewriteRulesPath)
	if err != nil {
		panic(err)
	}

	return &Authorization{
		store:         store,
		computedRules: rules,
	}
}
