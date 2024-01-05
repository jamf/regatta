// Copyright JAMF Software, LLC

package regattaserver

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jamf/regatta/regattapb"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestClusterServer_MemberList(t *testing.T) {
	r := require.New(t)
	cluster := &mockCLusterService{}
	cs := ClusterServer{
		Cluster: cluster,
	}

	t.Log("Get memberlist")
	cluster.On("MemberList", mock.Anything, mock.Anything).Return(&regattapb.MemberListResponse{}, nil)
	_, err := cs.MemberList(context.Background(), &regattapb.MemberListRequest{})
	r.NoError(err)
}

func TestClusterServer_Status(t *testing.T) {
	r := require.New(t)
	cluster := &mockCLusterService{}
	cs := ClusterServer{
		Cluster: cluster,
	}

	t.Log("Get status")
	cluster.On("Status", mock.Anything, mock.Anything).Return(&regattapb.StatusResponse{}, nil)
	_, err := cs.Status(context.Background(), &regattapb.StatusRequest{})
	r.NoError(err)

	t.Log("Get status with config")
	cs.Config = func() map[string]any {
		return map[string]any{
			"string": "bar",
			"int":    1,
			"nested": map[string]any{
				"string": "foo",
				"slice":  []string{"foo"},
			},
		}
	}
	st, err := cs.Status(context.Background(), &regattapb.StatusRequest{Config: true})
	r.NoError(err)
	want, err := json.Marshal(cs.Config())
	r.NoError(err)
	got, err := st.Config.MarshalJSON()
	r.NoError(err)
	r.JSONEq(string(want), string(got))
}

type mockCLusterService struct {
	mock.Mock
}

func (m *mockCLusterService) MemberList(ctx context.Context, request *regattapb.MemberListRequest) (*regattapb.MemberListResponse, error) {
	called := m.Called(ctx, request)
	return called.Get(0).(*regattapb.MemberListResponse), called.Error(1)
}

func (m *mockCLusterService) Status(ctx context.Context, request *regattapb.StatusRequest) (*regattapb.StatusResponse, error) {
	called := m.Called(ctx, request)
	return called.Get(0).(*regattapb.StatusResponse), called.Error(1)
}
