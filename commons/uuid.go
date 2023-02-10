/**
 * @Time    :2022/10/13 11:31
 * @Author  :Xiaoyu.Zhang
 */

package commons

import (
	"github.com/google/uuid"
	"strings"
)

// UUID 生成uuid
func UUID() string {
	id := uuid.New()
	return strings.ReplaceAll(id.String(), "-", "")
}

// StrUnique
/**
 *  @Description: 字符串去重
 *  @param str
 *  @return uniqueStr
 */
func StrUnique(str string) (uniqueStr string) {
	runes := []rune(str)
	runeMap := make(map[rune]struct{})
	var build strings.Builder
	for _, r := range runes {
		_, ok := runeMap[r]
		if !ok {
			build.WriteRune(r)
			runeMap[r] = struct{}{}
		}
	}
	return build.String()
}
