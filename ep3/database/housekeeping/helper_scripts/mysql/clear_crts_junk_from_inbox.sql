UPDATE pesstoObjects SET 
    marshallWorkflowLocation = "archive", 
    alertWorkflowLocation = "archived without alert" 
WHERE 
    pesstoObjectsId IN (
        SELECT pesstoObjectsId 
        FROM (
            SELECT pesstoObjectsId 
            FROM 
                pesstoObjects p, 
                transientBucket t 
            WHERE t.masterIDFlag = 1 AND 
                p.transientBucketId = t.transientBucketId AND 
                p.marshallWorkflowLocation = "inbox" AND 
                t.survey like "%crts%" AND ( 
                    t.transientTypePrediction like "Var%" OR 
                    t.transientTypePrediction like "HPM%"
                )
        ) as g
)


update transientBucket set limitingMag = 1 where survey like "CRTS%" and magnitude  > 10.;
