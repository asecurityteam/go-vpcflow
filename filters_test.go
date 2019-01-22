package vpcflow

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestMultiLogFileFilterIsProxyWithSingleElement(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()

	var fOne = NewMockLogFileFilter(ctrl)
	var fs = MultiLogFileFilter{fOne}

	var lf LogFile
	fOne.EXPECT().FilterLogFile(lf).Return(true)
	assert.Equal(t, true, fs.FilterLogFile(lf))
	fOne.EXPECT().FilterLogFile(lf).Return(false)
	assert.Equal(t, false, fs.FilterLogFile(lf))
}

func TestMultiLogFileFilterIsTrueWhenAllTrue(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()

	var mf = NewMockLogFileFilter(ctrl)
	var fs = MultiLogFileFilter{mf, mf, mf, mf, mf}

	var lf LogFile
	mf.EXPECT().FilterLogFile(lf).Return(true).AnyTimes()
	assert.Equal(t, true, fs.FilterLogFile(lf))
}

func TestMultiLogFileFilterIsFalseWhenAnyFalse(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()

	var mf = NewMockLogFileFilter(ctrl)
	var fs = MultiLogFileFilter{mf, mf, mf, mf, mf}

	var lf LogFile
	mf.EXPECT().FilterLogFile(lf).Return(true).Times(3)
	mf.EXPECT().FilterLogFile(lf).Return(false).Times(1)
	assert.Equal(t, false, fs.FilterLogFile(lf))
}

func TestLogFileTimeFilter_FilterLogFile(t *testing.T) {
	type fields struct {
		Start time.Time
		End   time.Time
	}
	type args struct {
		lf LogFile
	}
	// Use a static base time to prevent the small amount of
	// time it takes to create all the tests from creating
	// inconsistencies in the results.
	var base = time.Now()
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "before start",
			fields: fields{
				Start: base.Add(-time.Hour),
				End:   base.Add(time.Hour),
			},
			args: args{
				lf: LogFile{
					Timestamp: base.Add(-2 * time.Hour),
				},
			},
			want: false,
		},
		{
			name: "equal start",
			fields: fields{
				Start: base.Add(-time.Hour),
				End:   base.Add(time.Hour),
			},
			args: args{
				lf: LogFile{
					Timestamp: base.Add(-time.Hour),
				},
			},
			want: true,
		},
		{
			name: "in range",
			fields: fields{
				Start: base.Add(-time.Hour),
				End:   base.Add(time.Hour),
			},
			args: args{
				lf: LogFile{
					Timestamp: base.Add(-30 * time.Minute),
				},
			},
			want: true,
		},
		{
			name: "equal end",
			fields: fields{
				Start: base.Add(-time.Hour),
				End:   base.Add(time.Hour),
			},
			args: args{
				lf: LogFile{
					Timestamp: base.Add(time.Hour),
				},
			},
			want: true,
		},
		{
			name: "after end",
			fields: fields{
				Start: base.Add(-time.Hour),
				End:   base.Add(time.Hour),
			},
			args: args{
				lf: LogFile{
					Timestamp: base.Add(2 * time.Hour),
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := LogFileTimeFilter{
				Start: tt.fields.Start,
				End:   tt.fields.End,
			}
			if got := f.FilterLogFile(tt.args.lf); got != tt.want {
				t.Errorf("LogFileTimeFilter.FilterLogFile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLogFileRegionFilter_FilterLogFile(t *testing.T) {
	type fields struct {
		Region map[string]bool
	}
	type args struct {
		lf LogFile
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "empty set",
			fields: fields{
				Region: make(map[string]bool),
			},
			args: args{
				lf: LogFile{Region: endpoints.UsWest2RegionID},
			},
			want: false,
		},
		{
			name: "matching",
			fields: fields{
				Region: map[string]bool{
					endpoints.UsWest2RegionID: true,
				},
			},
			args: args{
				lf: LogFile{Region: endpoints.UsWest2RegionID},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := LogFileRegionFilter{
				Region: tt.fields.Region,
			}
			if got := f.FilterLogFile(tt.args.lf); got != tt.want {
				t.Errorf("LogFileRegionFilter.FilterLogFile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLogFileAccountFilter_FilterLogFile(t *testing.T) {
	type fields struct {
		Account map[string]bool
	}
	type args struct {
		lf LogFile
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "empty set",
			fields: fields{
				Account: make(map[string]bool),
			},
			args: args{
				lf: LogFile{Account: "123456789012"},
			},
			want: false,
		},
		{
			name: "matching",
			fields: fields{
				Account: map[string]bool{
					"123456789012": true,
				},
			},
			args: args{
				lf: LogFile{Account: "123456789012"},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := LogFileAccountFilter{
				Account: tt.fields.Account,
			}
			if got := f.FilterLogFile(tt.args.lf); got != tt.want {
				t.Errorf("LogFileAccountFilter.FilterLogFile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBucketFilterReturnsIfFirstPasses(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()

	var wrapped = NewMockBucketIterator(ctrl)
	var filter = NewMockLogFileFilter(ctrl)
	var iter = &BucketFilter{
		BucketIterator: wrapped,
		Filter:         filter,
	}

	var lf LogFile
	wrapped.EXPECT().Iterate().Return(true)
	wrapped.EXPECT().Current().Return(lf)
	filter.EXPECT().FilterLogFile(lf).Return(true)
	assert.Equal(t, true, iter.Iterate())
}

func TestBucketFilterReturnsIfFirstEmpty(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()

	var wrapped = NewMockBucketIterator(ctrl)
	var filter = NewMockLogFileFilter(ctrl)
	var iter = &BucketFilter{
		BucketIterator: wrapped,
		Filter:         filter,
	}

	wrapped.EXPECT().Iterate().Return(false)
	assert.Equal(t, false, iter.Iterate())
}

func TestBucketFilterReturnsAfterFindingPass(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()

	var wrapped = NewMockBucketIterator(ctrl)
	var filter = NewMockLogFileFilter(ctrl)
	var iter = &BucketFilter{
		BucketIterator: wrapped,
		Filter:         filter,
	}

	var lf LogFile
	wrapped.EXPECT().Iterate().Return(true)
	wrapped.EXPECT().Current().Return(lf)
	filter.EXPECT().FilterLogFile(lf).Return(false).Times(10)
	wrapped.EXPECT().Iterate().Return(true).Times(10)
	wrapped.EXPECT().Current().Return(lf).Times(10)
	filter.EXPECT().FilterLogFile(lf).Return(true)
	assert.Equal(t, true, iter.Iterate())
}

func TestBucketFilterReturnsAfterExhaustingIterator(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()

	var wrapped = NewMockBucketIterator(ctrl)
	var filter = NewMockLogFileFilter(ctrl)
	var iter = &BucketFilter{
		BucketIterator: wrapped,
		Filter:         filter,
	}

	var lf LogFile
	wrapped.EXPECT().Iterate().Return(true)
	wrapped.EXPECT().Current().Return(lf)
	filter.EXPECT().FilterLogFile(lf).Return(false).Times(10)
	wrapped.EXPECT().Iterate().Return(true).Times(10)
	wrapped.EXPECT().Current().Return(lf).Times(10)
	filter.EXPECT().FilterLogFile(lf).Return(false)
	wrapped.EXPECT().Iterate().Return(false)
	wrapped.EXPECT().Current().Return(lf)
	assert.Equal(t, false, iter.Iterate())
}
