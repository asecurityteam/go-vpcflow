package vpcflow

import (
	"time"
)

// LogFileFilter is used to inspect LogFile instances
// and determine if they are fit to be emitted from a
// BucketIterator
type LogFileFilter interface {
	FilterLogFile(LogFile) bool
}

// MultiLogFileFilter composes any number of filters into
// a single filter that returns false on the first failed
// filter or true if all filters pass.
type MultiLogFileFilter []LogFileFilter

// FilterLogFile executes all enclosed filters until one of them
// returns false.
func (f MultiLogFileFilter) FilterLogFile(lf LogFile) bool {
	for _, filter := range f {
		if !filter.FilterLogFile(lf) {
			return false
		}
	}
	return true
}

// LogFileTimeFilter applies an inclusive start/end time bound
// to all files.
type LogFileTimeFilter struct {
	Start time.Time
	End   time.Time
}

// FilterLogFile applies the time bound checks.
func (f LogFileTimeFilter) FilterLogFile(lf LogFile) bool {
	return ((lf.Timestamp.After(f.Start) || lf.Timestamp.Equal(f.Start)) &&
		(lf.Timestamp.Before(f.End) || lf.Timestamp.Equal(f.End)))
}

// LogFileRegionFilter reduces the set to only those from a
// particular set of regions.
type LogFileRegionFilter struct {
	Region map[string]bool
}

// FilterLogFile compares against the set of allowed regions.
func (f LogFileRegionFilter) FilterLogFile(lf LogFile) bool {
	return f.Region[lf.Region]
}

// BucketFilter is a BucketIterator wrapper that
// drops anything that fails the filter check.
type BucketFilter struct {
	Filter LogFileFilter
	BucketIterator
}

// Iterate will consume from the wrapped iterator until
// an element is found that passes the filter.
func (it *BucketFilter) Iterate() bool {
	var more = it.BucketIterator.Iterate()
	var curr = it.BucketIterator.Current()
	for more && !it.Filter.FilterLogFile(curr) {
		more = it.BucketIterator.Iterate()
		curr = it.BucketIterator.Current()
	}
	return more
}
