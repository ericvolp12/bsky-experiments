-- name: GetTopPosters :many
SELECT COUNT(p.id) AS post_count, a.handle, a.did AS author_did
FROM posts p
JOIN authors a ON p.author_did = a.did
WHERE a.did NOT IN (
    'did:plc:jlqiqmhalnu5af3pf56jryei',  -- Goose.art's Bot  - intern.goose.art
    'did:plc:vuwg6b5ashezjhh6lpnssljm',  -- Spammy Bot       - xnu.kr
    'did:plc:y5smfgzb3oitolqlln3atanl',  -- Retroid Bot      - who-up.bsky.social
    'did:plc:czze3j5772nu6gxdhben5i34',  -- Berduck          - berduck.deepfates.com
    'did:plc:4hqjfn7m6n5hno3doamuhgef',  -- Yui              - yui.syui.ai
    'did:plc:kwmcvt4maab47n7dgvepg4tr',  -- Timestamp Bot    - tick.bsky.social
    'did:plc:6smdztjrq7bjjlojkrnpcnxm',  -- Now Playinb Got  - worbler.bsky.social
    'did:plc:3tyx5envm7fms2jxgvq4pz6e',  -- Deleted Acc      - mavs.bsky.social
    'did:plc:zznz5dqjamwecp4yogdjugx2',  -- Deleted Acc      - shahbazi.bsky.social
    'did:plc:czcwobs37px7otals6umpd5j',  -- News Bot         - almir.bsky.social
    'did:plc:zqnx5g75q5ygxxxmoqfdcpsc'   -- Deleted Acc
)
GROUP BY a.did, a.handle
ORDER BY post_count DESC
LIMIT $1;
