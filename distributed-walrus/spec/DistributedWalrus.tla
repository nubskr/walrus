---- MODULE DistributedWalrus ----
EXTENDS Naturals, FiniteSets, Sequences

\* A high-level model of the distributed-walrus data plane. We abstract Walrus
\* storage as per-segment entry counts and treat the Raft-replicated metadata as
\* a single authoritative copy.

CONSTANTS
    Nodes, \* Set of node ids eligible to lead segments.
    Topics, \* Universe of topic names.
    SegIds, \* Finite set of allowed segment ids (e.g., 1..3).
    MaxSegmentEntries \* Rollover threshold used by the monitor.

VARIABLES
    topics, \* Set of topics that have been created.
    topicInfo, \* Metadata per topic: current segment, leader, sealed counts, per-segment leaders.
    wal, \* Per-topic, per-segment entry counts (writes are sequential appends).
    readers \* Per-topic read cursors (segment, delivered count within that segment).

TopicType ==
    [ currentSegment : SegIds,
      leader : Nodes,
      sealedCounts : [SegIds -> Nat], \* entries in sealed segments
      segmentLeaders : [SegIds -> Nodes],
      lastSealed : Nat ]

Vars == <<topics, topicInfo, wal, readers>>

Segments(t) ==
    { s \in SegIds : s <= topicInfo[t].currentSegment }

OpenSegment(t) ==
    topicInfo[t].currentSegment

FirstSeg == CHOOSE s \in SegIds : TRUE

SegmentsSafe(t) == IF t \in topics THEN Segments(t) ELSE {}
OpenSegmentSafe(t) == IF t \in topics THEN OpenSegment(t) ELSE FirstSeg

WalKey(t, s) == <<t, s>> \* informational only

RECURSIVE SumOver(_, _)

SumOver(S, f) ==
    IF S = {} THEN 0
    ELSE LET x == CHOOSE v \in S : TRUE
         IN f[x] + SumOver(S \ {x}, f)

WalCount(t, s) == IF t \in topics THEN Len(wal[t][s]) ELSE 0
ReaderSafe(t) == IF t \in topics THEN readers[t] ELSE [segment |-> FirstSeg, delivered |-> 0]

TypeOK ==
    /\ topics \subseteq Topics
    /\ topicInfo \in [topics -> TopicType]
    /\ wal \in [topics -> [SegIds -> Seq(Nat)]]
    /\ readers \in [topics -> [segment : SegIds, delivered : Nat]]
    /\ DOMAIN topicInfo = topics
    /\ DOMAIN wal = topics
    /\ DOMAIN readers = topics

Init ==
    /\ topics = {}
    /\ topicInfo = [t \in topics |-> [ currentSegment |-> FirstSeg,
                                       leader |-> CHOOSE n \in Nodes : TRUE,
                                       sealedCounts |-> [s \in SegIds |-> 0],
                                       segmentLeaders |-> [s \in SegIds |-> CHOOSE n \in Nodes : TRUE],
                                       lastSealed |-> 0 ]]
    /\ wal = [t \in topics |-> [s \in SegIds |-> <<>>]]
    /\ readers = [t \in topics |-> [segment |-> FirstSeg, delivered |-> 0]]

CreateTopic(t, leader) ==
    /\ t \in Topics \ topics
    /\ leader \in Nodes
    /\ topics' = topics \cup {t}
    /\ topicInfo' =
        [ x \in topics' |->
            IF x = t THEN
                [ currentSegment |-> FirstSeg,
                  leader |-> leader,
                  sealedCounts |-> [s \in SegIds |-> 0],
                  segmentLeaders |-> [s \in SegIds |-> leader],
                  lastSealed |-> 0 ]
            ELSE topicInfo[x] ]
    /\ wal' = [ x \in topics' |->
                IF x = t THEN [s \in SegIds |-> <<>>] ELSE wal[x] ]
    /\ readers' = [ x \in topics' |->
                    IF x = t THEN [segment |-> FirstSeg, delivered |-> 0] ELSE readers[x] ]

\* Renamed to avoid clash with any built-in Append
WalAppend(n, t) ==
    /\ t \in topics
    /\ n = topicInfo[t].leader
    /\ LET seg == OpenSegment(t)
       IN wal' = [wal EXCEPT ![t][seg] = Append(@, Len(@) + 1)]
    /\ UNCHANGED <<topics, topicInfo, readers>>

Rollover(t, newLeader) ==
    /\ t \in topics
    /\ newLeader \in Nodes
    /\ LET st == topicInfo[t]
       IN /\ WalCount(t, st.currentSegment) >= MaxSegmentEntries
          /\ st.currentSegment + 1 \in SegIds
          /\ topicInfo' =
                [topicInfo EXCEPT
                    ![t] =
                        [ currentSegment |-> st.currentSegment + 1,
                          leader |-> newLeader,
                          sealedCounts |-> [st.sealedCounts EXCEPT
                                                ![st.currentSegment] = WalCount(t, st.currentSegment)],
                          segmentLeaders |-> [st.segmentLeaders EXCEPT
                                                ![st.currentSegment] = st.leader,
                                                ![st.currentSegment + 1] = newLeader],
                          lastSealed |-> st.lastSealed + WalCount(t, st.currentSegment) ]]
          /\ wal' = [wal EXCEPT ![t][st.currentSegment + 1] = <<>>]
          /\ readers' = readers
          /\ topics' = topics

Read(t) ==
    /\ t \in topics
    /\ LET st  == topicInfo[t]
           cur == readers[t]
           seg == IF cur.segment = 0 THEN 1 ELSE cur.segment
       IN IF seg < st.currentSegment /\ cur.delivered >= st.sealedCounts[seg] THEN
             readers' = [readers EXCEPT ![t] = [segment |-> seg + 1, delivered |-> 0]]
          ELSE IF cur.delivered < WalCount(t, seg) THEN
             readers' = [readers EXCEPT ![t] = [segment |-> seg, delivered |-> cur.delivered + 1]]
          ELSE
             readers' = readers
    /\ UNCHANGED <<topics, topicInfo, wal>>

Next ==
    \/ \E t \in Topics : \E n \in Nodes : CreateTopic(t, n)
    \/ \E t \in topics : \E n \in Nodes : WalAppend(n, t)
    \/ \E t \in topics : \E n \in Nodes : Rollover(t, n)
    \/ \E t \in topics : Read(t)

Spec ==
    Init /\ [][Next]_Vars /\ WF_Vars(Next)

InvDomainConsistency ==
    /\ DOMAIN topicInfo = topics
    /\ DOMAIN wal = topics
    /\ DOMAIN readers = topics

InvOpenLeaderMatchesMap ==
    \A t \in topics : topicInfo[t].segmentLeaders[OpenSegment(t)] = topicInfo[t].leader

InvNoWritesPastOpen ==
    \A t \in topics :
        \A s \in SegIds : s > OpenSegment(t) => Len(wal[t][s]) = 0

InvSealedCountsStable ==
    \A t \in topics :
        \A s \in Segments(t) : s < OpenSegment(t) =>
            /\ topicInfo[t].sealedCounts[s] = WalCount(t, s)
            /\ topicInfo[t].segmentLeaders[s] \in Nodes

InvLastSealedIsSum ==
    \A t \in topics :
        topicInfo[t].lastSealed =
            SumOver({s \in SegIds : s < OpenSegment(t)},
                    [s \in SegIds |-> topicInfo[t].sealedCounts[s]])

InvReadCursorWithinBounds ==
    \A t \in topics :
        LET cur == readers[t] IN
        LET seg == cur.segment IN
        /\ seg <= OpenSegment(t)
        /\ cur.delivered <=
            IF seg < OpenSegment(t)
                THEN topicInfo[t].sealedCounts[seg]
                ELSE WalCount(t, seg)

InvSeqOrder ==
    \A t \in topics :
        \A s \in Segments(t) :
            \A i \in 1 .. Len(wal[t][s]) : wal[t][s][i] = i

Invariants ==
    /\ TypeOK
    /\ InvDomainConsistency
    /\ InvOpenLeaderMatchesMap
    /\ InvNoWritesPastOpen
    /\ InvSealedCountsStable
    /\ InvLastSealedIsSum
    /\ InvReadCursorWithinBounds
    /\ InvSeqOrder

RolloverProgress ==
    [] ( \A t \in Topics :
            \A s \in SegIds :
                (t \in topics /\ s <= OpenSegmentSafe(t) /\ WalCount(t, s) >= MaxSegmentEntries /\ s + 1 \in SegIds)
                => <> (OpenSegmentSafe(t) > s) )

CursorAdvances(t, seg, delivered) ==
    (ReaderSafe(t).segment > seg)
    \/ (ReaderSafe(t).segment = seg /\ ReaderSafe(t).delivered > delivered)

ReadProgress ==
    [] ( \A t \in Topics :
            IF t \in topics THEN
                \A s \in SegIds :
                    (ReaderSafe(t).segment = s /\ ReaderSafe(t).delivered < WalCount(t, s))
                    => <> CursorAdvances(t, s, ReaderSafe(t).delivered)
            ELSE TRUE )

==== 
