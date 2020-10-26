UPDATE pesstoObjects p,
    transientBucketSummaries s 
SET 
    marshallWorkflowLocation = 'archive'
WHERE
    marshallWorkflowLocation = 'inbox'
        AND s.transientBucketId = p.transientBucketId
        AND currentMagnitude > 20.5;
