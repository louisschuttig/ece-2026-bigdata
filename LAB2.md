# Lab 2 : stockage objet avec S3

Compte rendu du lab [`03.object-storage/lab-2-s3.md`](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/03.object-storage/lab-2-s3.md), réalisé sur Onyxia (service `vscode-pyspark`) avec le bucket personnel `user-l-schuttig-ece` (backend SeaweedFS, endpoint `https://s3.seaweedfs.adm.adaltas.cloud`).

## Résultats par section

| Section | Résultat |
| --- | --- |
| Environnement | Profil AWS `default` configuré depuis Onyxia. Le bucket n'existait pas : il a été créé depuis l'explorateur de fichiers Onyxia. |
| s5cmd | v2.3.0 installé dans `~/.local/bin`, checksum SHA-256 vérifiée. |
| Upload, liste, download | `users.csv` (7 351 octets) envoyé dans `bronze/`, listé avec `aws s3` et `s5cmd`, re-téléchargé : fichiers identiques (`diff`). |
| Renommage, suppression | `mv` = copie + suppression. `rm` supprime l'objet. |
| Métadonnées | ETag = MD5 du fichier (`7289c046e686ab80e449aa9704638d35`). Métadonnées utilisateur `source`, `version`, `rows` ajoutées et relues avec `head-object`. |
| URLs présignées | URL GET lisible avec `curl` sans identifiants. URL PUT générée avec boto3. |
| Multipart | Fichier de 200 Mo : ETag suffixé `-25` (25 parties). Aucun upload incomplet. |
| Versioning | Activé. Deux versions de `users.csv` (7 351 puis 5 855 octets), la v1 restaurée avec `--version-id`. Après `rm`, un delete marker est créé et les versions restent présentes. |
| Lifecycle | Les 3 règles (`expire-large-after-30-days`, `delete-noncurrent-versions`, `abort-incomplete-multipart`) sont acceptées par SeaweedFS. |
| Bucket policy | La suppression dans `bronze/` est bien refusée (`AccessDenied`) tant que la policy est active. Policy retirée ensuite. |
| ACL | Lecture OK : un seul grant `admin` / `FULL_CONTROL`. |
| Cohérence | Objet lisible immédiatement après le `PUT`. La CLI affiche un avertissement en écrivant dans `/dev/null` (mise à jour de la date impossible), mais le téléchargement réussit. |
| Nettoyage | Toutes les versions et delete markers supprimés, versioning en `Suspended`, lifecycle supprimée. |
| Job Kubernetes | `bronze/orders.csv` (331 568 octets) et `bronze/users.csv` (7 351 octets) envoyés par le Job. ConfigMaps, Secret et Job supprimés après vérification. |

## Écart par rapport à l'énoncé : `limits.cpu`

Avec le YAML de l'énoncé, le Job reste bloqué (`0/1`) et aucun pod n'est créé. Les événements du Job indiquent :

```
pods "upload-bronze-xxxxx" is forbidden: failed quota: onyxia-quota: must specify limits.cpu for: upload
```

Le namespace Onyxia a un ResourceQuota qui porte sur `limits.cpu`. Kubernetes refuse donc tout conteneur qui ne déclare pas de limite CPU. Correction appliquée dans [`job-upload-bronze.yaml`](./job-upload-bronze.yaml) :

```yaml
limits:
  cpu: 500m
  memory: 256Mi
```

Diagnostic : `kubectl get pods -l job-name=upload-bronze` (aucun pod), puis `kubectl describe job upload-bronze` (événements `FailedCreate`).

## Réponses aux questions

**Pourquoi les identifiants sont-ils dans un Secret et pas dans la ConfigMap ?**
Une ConfigMap sert à la configuration non sensible (endpoint, région, nom du bucket) et se lit en clair. Un Secret est fait pour les données sensibles : ses droits d'accès RBAC peuvent être restreints séparément, il peut être chiffré au repos dans etcd et il s'intègre aux gestionnaires de secrets (Vault, External Secrets). Séparer les deux évite aussi d'exposer les clés quand on partage ou versionne la configuration.

**Les identifiants Onyxia sont temporaires. Que se passe-t-il si le Job est relancé demain ? Comment une plateforme de production fournit-elle des identifiants à un Job ?**
Les identifiants sont des jetons STS de courte durée (12 h maximum ici). Demain, le Secret contiendrait un jeton expiré et le Job échouerait (`ExpiredToken` / `AccessDenied`). En production, on ne copie pas de clés à la main : le ServiceAccount Kubernetes du pod est fédéré avec le fournisseur d'identité (IRSA sur AWS, Workload Identity sur GCP, ou OIDC vers STS via `AssumeRoleWithWebIdentity`). Le pod obtient ainsi des identifiants courts, renouvelés automatiquement. À défaut, on utilise un compte de service dédié dont les clés sont stockées dans un coffre (Vault) avec rotation automatique.

**Comment transformer ce Job en ingestion quotidienne ?**
On remplace le `Job` par un `CronJob` (par exemple `schedule: "0 2 * * *"`) qui reprend le même modèle de pod, avec `concurrencyPolicy: Forbid` et un historique limité. Il faut aussi :

- partitionner les clés par date (`bronze/orders/date=YYYY-MM-DD/orders.csv`) pour ne pas écraser les données précédentes ;
- lire les données depuis le système source plutôt que depuis une ConfigMap (limitée à 1 Mio) ;
- fournir des identifiants qui se renouvellent seuls (voir la question précédente).

Pour des dépendances entre plusieurs tâches, on passe à un orchestrateur (Airflow, Argo Workflows).
