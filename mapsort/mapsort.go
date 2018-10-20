package mapsort

import "sort"

type byValue struct {
	m map[string]int
	s []string
}

func (sm *byValue) Len() int {
	return len(sm.m)
}

func (sm *byValue) Less(i, j int) bool {
	return sm.m[sm.s[i]] > sm.m[sm.s[j]]
}

func (sm *byValue) Swap(i, j int) {
	sm.s[i], sm.s[j] = sm.s[j], sm.s[i]
}

func ByValue(m map[string]int, reverse bool) []string {
	sm := new(byValue)
	sm.m = m
	sm.s = make([]string, len(m))
	i := 0

	for key, _ := range m {
		sm.s[i] = key
		i++
	}

	if reverse {
		sort.Sort(sort.Reverse(sm))
	} else {
		sort.Sort(sm)
	}

	return sm.s
}
