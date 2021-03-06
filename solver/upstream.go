package solver

import (
	"github.com/hbagdi/deck/crud"
	"github.com/hbagdi/deck/diff"
	"github.com/hbagdi/deck/state"
	"github.com/hbagdi/go-kong/kong"
)

// upstreamCRUD implements crud.Actions interface.
type upstreamCRUD struct {
	client *kong.Client
}

func upstreamFromStuct(arg diff.Event) *state.Upstream {
	upstream, ok := arg.Obj.(*state.Upstream)
	if !ok {
		panic("unexpected type, expected *state.upstream")
	}
	return upstream
}

// Create creates a Upstream in Kong.
// The arg should be of type diff.Event, containing the upstream to be created,
// else the function will panic.
// It returns a the created *state.Upstream.
func (s *upstreamCRUD) Create(arg ...crud.Arg) (crud.Arg, error) {
	event := eventFromArg(arg[0])
	upstream := upstreamFromStuct(event)
	createdUpstream, err := s.client.Upstreams.Create(nil, &upstream.Upstream)
	if err != nil {
		return nil, err
	}
	return &state.Upstream{Upstream: *createdUpstream}, nil
}

// Delete deletes a Upstream in Kong.
// The arg should be of type diff.Event, containing the upstream to be deleted,
// else the function will panic.
// It returns a the deleted *state.Upstream.
func (s *upstreamCRUD) Delete(arg ...crud.Arg) (crud.Arg, error) {
	event := eventFromArg(arg[0])
	upstream := upstreamFromStuct(event)
	err := s.client.Upstreams.Delete(nil, upstream.ID)
	if err != nil {
		return nil, err
	}
	return upstream, nil
}

// Update updates a Upstream in Kong.
// The arg should be of type diff.Event, containing the upstream to be updated,
// else the function will panic.
// It returns a the updated *state.Upstream.
func (s *upstreamCRUD) Update(arg ...crud.Arg) (crud.Arg, error) {
	event := eventFromArg(arg[0])
	upstream := upstreamFromStuct(event)

	updatedUpstream, err := s.client.Upstreams.Create(nil, &upstream.Upstream)
	if err != nil {
		return nil, err
	}
	return &state.Upstream{Upstream: *updatedUpstream}, nil
}
