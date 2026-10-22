# Aide-mémoire de démo

À garder ouvert pendant la soutenance (VS Code : Cmd+Shift+V pour l'aperçu).
Pour chaque question du jury : **la façon visuelle** (navigateur) et **la façon terminal**.

## Avant de commencer (15 min avant)

```bash
cd ~/Desktop/Sayf/marketplace-analytics
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
./scripts/demo.sh
```

| Outil | Adresse | Identifiants |
|---|---|---|
| Airflow | localhost:8080 | airflow / airflow |
| Metabase | localhost:3000 | admin@maelys.local / Admin2026! |
| Grafana | localhost:3001 | admin / admin |
| Prometheus | localhost:9090 | — |
| MySQL (site) | terminal | backoffice_user / backoffice_password_2026 |
| Data Warehouse | terminal | dwh_user / dwh_password_2026 |

---

## « Montrez-nous la base MySQL du site »

**Visuel** — Metabase → menu de gauche **Données → Bases de données → Backoffice Maelys (MySQL)** → cliquer sur `orders` : les ventes s'affichent.

**Terminal**
```bash
docker compose exec mysql-backoffice mysql -ubackoffice_user -p --default-character-set=utf8mb4 backoffice
```
```sql
SHOW TABLES;
SELECT COUNT(*) FROM orders;
SELECT * FROM orders WHERE order_id = 'ORD-20261020-000001';
SELECT o.order_id, s.name AS vendeur, p.name AS produit, o.total_amount
FROM orders o JOIN sellers s USING (seller_id) JOIN products p USING (product_id)
LIMIT 5;
exit
```
*À dire :* « C'est la base du site, transactionnelle. On n'y accède jamais directement dans le pipeline : on passe par l'API. »

## « Comment Airflow récupère les données ? » (l'API)

```bash
curl -i "http://localhost:5002/orders?date=2026-10-20" | head -1          # 401 : refusé sans token
curl -s -H "Authorization: Bearer formation-token-2026" \
     "http://localhost:5002/orders?date=2026-10-20" | head -c 300           # les ventes en JSON
```
*À dire :* « Sans badge, refusé. Airflow présente le token rangé dans sa Connection, jamais écrit dans le code. »
(Si `Service temporarily unavailable` : panne simulée à 5 %, relancer — c'est ce que gère le retry.)

## « Montrez-nous les données dans MinIO »

```bash
./scripts/minio.sh               # les journées archivées + la taille du data lake
./scripts/minio.sh 2026-10-20    # le début du fichier JSON brut du 20 octobre
```
*À dire :* « Un fichier JSON par jour, tel que reçu. La tâche de chargement relit ce fichier ; et on peut tout rejouer depuis ici. »

## « Montrez-nous le Data Warehouse »

**Visuel** — Metabase → **Données → Bases de données → DWH Maelys** : les 3 zones `staging`, `dwh`, `analytics` → cliquer sur une zone puis une table.

**Terminal**
```bash
docker compose exec postgres-dwh psql -U dwh_user -d dwh
```
```sql
\dn                 -- les 3 zones
\dt staging.*       -- zone de vérification
\dt dwh.*           -- le schéma en étoile
\dt analytics.*     -- les totaux
\q
```

## « Montrez-nous une phase précise »

Dans psql (`docker compose exec postgres-dwh psql -U dwh_user -d dwh`) :

```sql
-- staging : les ventes reçues, avant contrôle
SELECT order_id, dt, total_amount, status FROM staging.orders WHERE dt = '2026-10-20' LIMIT 5;

-- dwh : la même vente rangée, avec ses fiches (fait + dimensions)
SELECT f.order_id, d.day_name, s.seller_name, p.product_name, p.category, f.total_amount, f.commission_amount
FROM dwh.fact_orders f
JOIN dwh.dim_seller  s USING (seller_id)
JOIN dwh.dim_product p USING (product_id)
JOIN dwh.dim_date    d USING (dt)
WHERE f.order_id = 'ORD-20261020-000001';

-- analytics : les totaux que lit Metabase
SELECT dt, total_orders, total_revenue, total_commission, avg_basket
FROM analytics.daily_summary ORDER BY dt DESC LIMIT 5;
```

## « Comment savez-vous que rien n'est perdu ? »

```sql
-- dans MySQL
SELECT COUNT(*), SUM(total_amount) FROM orders WHERE order_date = '2026-10-20';           -- 8059 | 140768.32
-- dans le Data Warehouse (psql)
SELECT COUNT(*), SUM(total_amount) FROM staging.orders  WHERE dt = '2026-10-20';           -- 8059 | 140768.32
SELECT COUNT(*), SUM(total_amount) FROM dwh.fact_orders WHERE dt = '2026-10-20';           -- 8059 | 140768.32
SELECT total_orders, total_revenue FROM analytics.daily_summary WHERE dt = '2026-10-20';   -- 8059 | 134082.55
SELECT COUNT(*) - COUNT(DISTINCT order_id) AS doublons FROM dwh.fact_orders;               -- 0
```
*À dire :* « Le même nombre de ventes partout. Le CA est plus bas car il exclut les commandes annulées et remboursées. »

## « Et si on relance le pipeline deux fois ? » (idempotence)

```bash
./scripts/test_idempotence.sh 2026-04-07     # 7350 = 7350
```
*À montrer aussi :* Airflow → `transform_fact_orders` → onglet **Rendered Templates** : le `DELETE` puis l'`INSERT` de la journée.

## « Montrez-nous le contrôle qualité »

```bash
docker compose exec airflow-worker airflow dags test marketplace_orders_ingest_daily 2026-12-24
```
Puis Airflow → le run rouge → `data_quality_staging` → **Logs** : `no_future_date ❌ KO (10673 violation(s))`.
*À dire :* « Date future : les ventes sont bloquées en staging, la branche d'alerte s'exécute, rien n'entre dans le Data Warehouse. »
(Ménage ensuite, dans psql : `DELETE FROM staging.orders WHERE dt > CURRENT_DATE;`)

## « D'où vient ce chiffre du dashboard ? »

Metabase → cliquer sur le **titre** du graphique → **Ouvrir l'éditeur** → le SQL. Le relancer dans psql donne le même chiffre.

**Écrire sa propre requête dans Metabase :** bouton **+ Nouveau → Requête SQL** → choisir la base → taper le SQL → ▶.

## « Montrez-nous les DAGs et ce qu'ils font »

Airflow → **Dags** → `marketplace_orders_ingest_daily` → icône **graphe** (en haut à gauche) → les 7 tâches.
Clic sur une case → **Logs** (ce qu'elle a fait), **XCom** (ce qu'elle transmet), **Rendered Templates** (le SQL exécuté).

```bash
docker compose exec airflow-worker airflow dags list
docker compose exec airflow-worker airflow dags list-runs marketplace_orders_ingest_daily | head
```

## « Montrez-nous Redis »

```bash
docker compose exec redis redis-cli LLEN default     # tâches en attente (0 au repos)
docker compose exec redis redis-cli MONITOR          # en direct ; cliquer ▶ Trigger sur un DAG ; Ctrl+C
```

## « Montrez-nous la supervision »

Prometheus → **Status → Target health** : 3 cibles UP.
Grafana → **Dashboards → MarketPlace Analytics → Supervision du pipeline** ; **Alerting → Alert rules → DAG MarketPlace en échec**.

## « Les tests ? »

```bash
docker compose exec airflow-worker pytest tests/ -v     # 21 passed
```

## Pour finir

```bash
docker compose stop        # arrête tout, garde les données (jamais « down -v »)
```
