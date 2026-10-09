# Lab 2: Object storage with S3

Report for the lab [`03.object-storage/lab-2-s3.md`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/03.object-storage/lab-2-s3.md), completed on Onyxia (`vscode-pyspark` service) with the personal bucket `user-l-schuttig-ece` (SeaweedFS backend, endpoint `https://s3.seaweedfs.adm.adaltas.cloud`).

## Results by section

| Section | Result |
| --- | --- |
| Environment | AWS `default` profile configured from Onyxia. The bucket did not exist: it was created from the Onyxia file explorer. |
| s5cmd | v2.3.0 installed in `~/.local/bin`, SHA-256 checksum verified. |
| Upload, list, download | `users.csv` (7,351 bytes) uploaded to `bronze/`, listed with `aws s3` and `s5cmd`, downloaded again: files are identical (`diff`). |
| Move, delete | `mv` is a copy followed by a delete. `rm` removes the object. |
| Metadata | ETag equals the MD5 of the file (`7289c046e686ab80e449aa9704638d35`). User metadata `source`, `version` and `rows` added and read back with `head-object`. |
| Presigned URLs | GET URL readable with `curl` without credentials. PUT URL generated with boto3. |
| Multipart | 200 MB file: ETag suffixed with `-25` (25 parts). No incomplete upload left. |
| Versioning | Enabled. Two versions of `users.csv` (7,351 then 5,855 bytes), version 1 restored with `--version-id`. After `rm`, a delete marker is created and the versions remain. |
| Lifecycle | All 3 rules (`expire-large-after-30-days`, `delete-noncurrent-versions`, `abort-incomplete-multipart`) are accepted by SeaweedFS. |
| Bucket policy | Deletion under `bronze/` is denied (`AccessDenied`) while the policy is active. The policy was removed afterwards. |
| ACL | Read OK: a single `admin` / `FULL_CONTROL` grant. |
| Consistency | Object readable immediately after the `PUT`. The CLI prints a warning when writing to `/dev/null` (it cannot update the file timestamp), but the download succeeds. |
| Cleanup | All versions and delete markers removed, versioning set to `Suspended`, lifecycle configuration deleted. |
| Kubernetes Job | `bronze/orders.csv` (331,568 bytes) and `bronze/users.csv` (7,351 bytes) uploaded by the Job. ConfigMaps, Secret and Job deleted after verification. |

## Deviation from the instructions: `limits.cpu`

With the YAML from the instructions, the Job stays stuck (`0/1`) and no pod is created. The Job events show:

```
pods "upload-bronze-xxxxx" is forbidden: failed quota: onyxia-quota: must specify limits.cpu for: upload
```

The Onyxia namespace has a ResourceQuota on `limits.cpu`, so Kubernetes rejects any container that does not declare a CPU limit. Fix applied in [`job-upload-bronze.yaml`](./job-upload-bronze.yaml):

```yaml
limits:
  cpu: 500m
  memory: 256Mi
```

Diagnosis: `kubectl get pods -l job-name=upload-bronze` (no pod), then `kubectl describe job upload-bronze` (`FailedCreate` events).

## Answers to the questions

**Why are the credentials stored in a Secret and not in the ConfigMap?**
A ConfigMap holds non-sensitive configuration (endpoint, region, bucket name) and is readable in plain text. A Secret is meant for sensitive data: its RBAC permissions can be restricted separately, it can be encrypted at rest in etcd, and it integrates with secret managers (Vault, External Secrets). Keeping them separate also prevents leaking the keys when the configuration is shared or versioned.

**Onyxia credentials are temporary. What happens if the Job runs again tomorrow? How would a production platform provide credentials to a Job?**
The credentials are short-lived STS tokens (12 hours at most here). Tomorrow, the Secret would contain an expired token and the Job would fail (`ExpiredToken` / `AccessDenied`). In production, keys are never copied by hand: the pod's Kubernetes ServiceAccount is federated with the identity provider (IRSA on AWS, Workload Identity on GCP, or OIDC to STS through `AssumeRoleWithWebIdentity`). The pod then gets short-lived credentials that are renewed automatically. Otherwise, a dedicated service account is used, with its keys stored in a vault (Vault) and rotated automatically.

**How would you turn this Job into a daily ingestion?**
Replace the `Job` with a `CronJob` (for example `schedule: "0 2 * * *"`) that reuses the same pod template, with `concurrencyPolicy: Forbid` and a limited history. In addition:

- partition the keys by date (`bronze/orders/date=YYYY-MM-DD/orders.csv`) so previous data is not overwritten;
- read the data from the source system instead of a ConfigMap (limited to 1 MiB);
- provide credentials that renew themselves (see the previous question).

When several tasks depend on each other, move to an orchestrator (Airflow, Argo Workflows).
