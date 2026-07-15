# Blue-Green Deployment Design (fill this in)

> Deliverable for Part B. Explain the strategy and show it working with the
> provided `docker-compose.blue-green.yml` + `switch_traffic.sh`. No Kubernetes.

## 1. Strategy
- What "blue" and "green" are here, and which one serves live traffic.
- How a new version is brought up alongside the old one.

## 2. Cutover
- How `switch_traffic.sh` flips nginx from one version to the other.
- Why the reload is zero-downtime (in-flight requests drain).

## 3. Health gate & rollback
- The health check performed before cutover.
- How you roll back (hint: run the switch again).

## 4. How this maps to Kubernetes (1 short paragraph)
- What the nginx upstream + reload becomes in K8s (Service selector / two
  Deployments). You will build the K8s version in a later unit.

## 5. Evidence
- Screenshot / terminal capture of a switch with no dropped requests.
