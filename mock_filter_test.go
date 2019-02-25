// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/asecurityteam/go-vpcflow (interfaces: LogFileFilter)

// nolint
package vpcflow

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockLogFileFilter is a mock of LogFileFilter interface
type MockLogFileFilter struct {
	ctrl     *gomock.Controller
	recorder *MockLogFileFilterMockRecorder
}

// MockLogFileFilterMockRecorder is the mock recorder for MockLogFileFilter
type MockLogFileFilterMockRecorder struct {
	mock *MockLogFileFilter
}

// NewMockLogFileFilter creates a new mock instance
func NewMockLogFileFilter(ctrl *gomock.Controller) *MockLogFileFilter {
	mock := &MockLogFileFilter{ctrl: ctrl}
	mock.recorder = &MockLogFileFilterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockLogFileFilter) EXPECT() *MockLogFileFilterMockRecorder {
	return m.recorder
}

// FilterLogFile mocks base method
func (m *MockLogFileFilter) FilterLogFile(arg0 LogFile) bool {
	ret := m.ctrl.Call(m, "FilterLogFile", arg0)
	ret0, _ := ret[0].(bool)
	return ret0
}

// FilterLogFile indicates an expected call of FilterLogFile
func (mr *MockLogFileFilterMockRecorder) FilterLogFile(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FilterLogFile", reflect.TypeOf((*MockLogFileFilter)(nil).FilterLogFile), arg0)
}
