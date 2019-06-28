package zanzibar

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

const configExt = "yaml"

// LoadRewriteRules gets rules from all yaml files inside the given directory
func LoadRewriteRules(root string) (map[string]UsersetRewrite, error) {
	res := make(map[string]UsersetRewrite)
	if root == "" {
		return res, nil
	}

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		index := strings.LastIndex(path, ".")
		if index == -1 {
			return nil
		}

		ext := path[index+1:]
		if ext != configExt {
			return nil
		}

		blob, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}

		var t NamespaceConfig
		err = yaml.Unmarshal(blob, &t)
		if err != nil {
			return err
		}

		namespace := t.Name
		for _, rel := range t.Relations {
			res[namespace+"#"+rel.Name] = rel.UsersetRewrite
		}

		return nil
	})

	return res, err
}
