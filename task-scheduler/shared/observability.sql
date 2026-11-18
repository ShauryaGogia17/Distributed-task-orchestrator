WITH outcomes AS (
    SELECT COALESCE(payload->>'type','unknown') AS type, payload->>'status' AS status,
      CASE WHEN jsonb_typeof(payload->'duration_seconds')='number'
        THEN GREATEST(0,(payload->>'duration_seconds')::double precision) END AS duration
    FROM outbox WHERE topic='tasks.results' AND payload->>'status' IN ('SUCCEEDED','RETRYING','FAILED')
), processed AS (SELECT type,status,count(*) AS value FROM outcomes GROUP BY type,status),
latency AS (SELECT type,count(*) AS count,COALESCE(sum(duration),0) AS sum,
    jsonb_build_object('0.005', count(*) FILTER (WHERE duration<=0.005), '0.01', count(*) FILTER (WHERE duration<=0.01), '0.025', count(*) FILTER (WHERE duration<=0.025), '0.05', count(*) FILTER (WHERE duration<=0.05), '0.1', count(*) FILTER (WHERE duration<=0.1), '0.25', count(*) FILTER (WHERE duration<=0.25), '0.5', count(*) FILTER (WHERE duration<=0.5), '1', count(*) FILTER (WHERE duration<=1), '2.5', count(*) FILTER (WHERE duration<=2.5), '5', count(*) FILTER (WHERE duration<=5), '10', count(*) FILTER (WHERE duration<=10), '30', count(*) FILTER (WHERE duration<=30), '60', count(*) FILTER (WHERE duration<=60), '120', count(*) FILTER (WHERE duration<=120),'+Inf',count(*)) AS buckets
    FROM outcomes WHERE duration IS NOT NULL GROUP BY type),
queue AS (SELECT task_type AS type,count(*) FILTER (WHERE status='OPEN') AS value FROM dead_letters GROUP BY task_type),
diagnoses AS (SELECT COALESCE(ai_diagnosis->>'category','unknown') AS category,
    COALESCE(ai_diagnosis->>'action','ESCALATE') AS action,count(*) AS value
    FROM dead_letters WHERE agent_processed_at IS NOT NULL AND ai_diagnosis IS NOT NULL
    GROUP BY 1,2),
audit_usage AS (SELECT
    COALESCE(sum((details->'usage'->>'input_tokens')::numeric) FILTER (WHERE event='LLM_RESPONSE'),0) AS input,
    COALESCE(sum((details->'usage'->>'output_tokens')::numeric) FILTER (WHERE event='LLM_RESPONSE'),0) AS output,
    COALESCE(sum((details->>'charged_tokens')::numeric) FILTER (WHERE event IN ('LLM_RESPONSE','LLM_FAILED')),0) AS charged,
    COALESCE(sum(CASE WHEN event='LLM_FAILED' THEN (details->>'charged_micro_usd')::numeric
        WHEN event='LLM_RESPONSE' THEN (details->>'micro_usd')::numeric ELSE 0 END),0)/1000000 AS cost
    FROM agent_audit),
known_types AS (SELECT type FROM tasks UNION SELECT task_type FROM dead_letters)
SELECT jsonb_build_object(
 'submitted',(SELECT count(*) FROM tasks),
 'processed',COALESCE((SELECT jsonb_agg(to_jsonb(p)) FROM processed p),'[]'::jsonb),
 'latency',COALESCE((SELECT jsonb_agg(to_jsonb(l)) FROM latency l),'[]'::jsonb),
 'dlq',COALESCE((SELECT jsonb_agg(to_jsonb(q)) FROM queue q),'[]'::jsonb),
 'diagnoses',COALESCE((SELECT jsonb_agg(to_jsonb(d)) FROM diagnoses d),'[]'::jsonb),
 'tokens',(SELECT to_jsonb(a) FROM audit_usage a),
 'types',COALESCE((SELECT jsonb_agg(type) FROM known_types),'[]'::jsonb),
 'outbox_lag',(SELECT COALESCE(GREATEST(0,extract(epoch FROM (clock_timestamp()-min(created_at)))),0) FROM outbox WHERE published_at IS NULL),
 'outbox_pending',(SELECT count(*) FROM outbox WHERE published_at IS NULL),
 'running',(SELECT count(*) FROM tasks WHERE status='RUNNING'),
 'running_age',(SELECT COALESCE(GREATEST(0,extract(epoch FROM (clock_timestamp()-min(updated_at)))),0) FROM tasks WHERE status='RUNNING'),
 'stuck_running',(SELECT count(*) FROM tasks WHERE status='RUNNING' AND updated_at<clock_timestamp()-(:stuck_seconds * interval '1 second'))
)
