package kafkaauth

import (
	"context"
	"crypto/tls"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	awssasl "github.com/twmb/franz-go/pkg/sasl/aws"
)

// returns the kgo options needed to authenticate to an MSK Serverless
// cluster over SASL/IAM + TLS. Local docker-compose's Kafka broker is
// plaintext, so this only engages when KAFKA_AUTH_MODE=iam - set on the AWS
// infra-config ConfigMap, unset (and therefore a no-op) everywhere else.
func IAMOpts(ctx context.Context) ([]kgo.Opt, error) {
	if os.Getenv("KAFKA_AUTH_MODE") != "iam" {
		return nil, nil
	}
	// load aws sdk default credential chain which loads env vars, injected OIDC token
	// picks up token, calls sts:assumeRoleWithWebIdentity, gets the access token, secret (temp ~1 hour rotated)
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	mechanism := sasl.Mechanism(awssasl.ManagedStreamingIAM(func(ctx context.Context) (awssasl.Auth, error) {
		// if sts token still valid use it, else if expired, read updated OIDC token from disk
		// calls sts with updated token and gets new temp credentials for connecting with msk
		creds, err := cfg.Credentials.Retrieve(ctx)
		if err != nil {
			return awssasl.Auth{}, err
		}

		// sign payload with these creds to connect msk
		return awssasl.Auth{
			AccessKey:    creds.AccessKeyID,
			SecretKey:    creds.SecretAccessKey,
			SessionToken: creds.SessionToken,
		}, nil
	}))

	// need tls on port 9098, mechanism generated applied to client options
	return []kgo.Opt{
		kgo.DialTLSConfig(new(tls.Config)),
		kgo.SASL(mechanism),
	}, nil
}
