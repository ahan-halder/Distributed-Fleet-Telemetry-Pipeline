package server

import (
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// bigtableWriteErr wraps a Bigtable write failure with retry guidance.
func bigtableWriteErr(err error, retryAfter time.Duration) error {
	st, _ := status.New(codes.Unavailable, "bigtable write failed: transient error").
		WithDetails(
			&errdetails.RetryInfo{
				RetryDelay: durationpb.New(retryAfter),
			},
			&errdetails.ErrorInfo{
				Reason: "BIGTABLE_WRITE_FAILURE",
				Domain: "fleet.telemetry.v1",
			},
		)
	return st.Err()
}

// validationErr returns field-level validation failures to the agent.
func validationErr(violations []*errdetails.BadRequest_FieldViolation) error {
	st, _ := status.New(codes.InvalidArgument, "metric frame validation failed").
		WithDetails(&errdetails.BadRequest{FieldViolations: violations})
	return st.Err()
}
