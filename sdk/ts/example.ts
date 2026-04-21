import { RunqClient } from './client';

async function main() {
  const client = new RunqClient('http://localhost:8080', 'tenant-token');

  const me = await client.authMe();
  console.log('auth me', me);

  const created = await client.createJob({
    name: 'sdk-job',
    tenant_id: 'tenant-api',
    queue: 'default',
    kind: 'http',
    payload: { url: 'https://example.internal/task' },
  });
  console.log('created', created);

  const jobs = await client.listJobs({ tenant_id: 'tenant-api' });
  console.log('jobs', jobs.jobs.length);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
