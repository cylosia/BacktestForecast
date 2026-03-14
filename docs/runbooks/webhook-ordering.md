# Runbook: Stripe Webhook Ordering Incidents

## Symptoms
- User reports being downgraded to free tier despite active subscription
- Billing audit events show subscription status flip-flops
- DLQ contains stale subscription events

## Diagnosis
1. Check the user's billing state:
   ```sql
   SELECT id, plan_tier, subscription_status, stripe_subscription_id,
          subscription_current_period_end, plan_updated_at
   FROM users WHERE clerk_user_id = '<clerk_id>';
   ```

2. Check audit events for billing changes:
   ```sql
   SELECT event_type, metadata_json, created_at
   FROM audit_events
   WHERE user_id = '<user_uuid>' AND event_type LIKE 'billing.%'
   ORDER BY created_at DESC LIMIT 20;
   ```

3. Check Stripe dashboard for webhook delivery history

## Resolution
1. If the user has an active Stripe subscription but shows as `free`:
   ```sql
   UPDATE users SET
     plan_tier = '<correct_tier>',
     subscription_status = 'active'
   WHERE id = '<user_uuid>';
   ```

2. Verify by checking Stripe subscription status via API

## Prevention
- The stale subscription guard (added in audit round 3) rejects webhooks
  for old subscription IDs when a newer subscription is active
- The out-of-order webhook guard skips events with older period_end dates
- Both guards log skip events for monitoring

## Monitoring
- Alert: `BillingSubscriptionOutOfOrderWebhookSkipped` in Grafana
- Log: `billing.subscription.stale_subscription_skipped`
- DLQ: Check `/admin/dlq` for failed billing events
