package runqsdk

import (
	"context"
	"fmt"
)

func ExampleClient() {
	client := NewClient("http://localhost:8080", "tenant-token")
	ctx := context.Background()

	_, _ = client.AuthMe(ctx)
	_, _ = client.ListJobs(ctx, map[string]string{"tenant_id": "tenant-api"})
	_, _ = client.LookupJobByDedupeKey(ctx, "tenant-api", "example-key")

	fmt.Println("runq sdk example")
	// Output: runq sdk example
}
