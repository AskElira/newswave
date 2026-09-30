from __future__ import annotations

from datetime import UTC, datetime

from newswave.market.subscriptions import SlotPriority as P
from newswave.market.subscriptions import SubscriptionManager

NOW = datetime(2026, 1, 2, 15, tzinfo=UTC)


def mgr(cap=5, shadow_max=8):
    return SubscriptionManager(cap=cap, shadow_max=shadow_max)  # SPY, QQQ reserved -> 3 free slots


def test_desired_includes_reserved_and_diff():
    m = mgr()
    assert m.desired() == {"SPY", "QQQ"}
    m.request("AAPL", "a", P.PRODUCTION, 0.9, NOW)
    assert m.diff({"SPY", "OLD"}) == ({"QQQ", "AAPL"}, {"OLD"})


def test_capacity_and_no_eviction_on_equal_priority():
    m = mgr()
    for s in ("A", "B", "C"):
        assert m.request(s, s, P.PRODUCTION, 0.8, NOW)
    assert not m.request("D", "D", P.PRODUCTION, 0.99, NOW)
    assert len(m.desired()) == 5 and m.drain_evicted() == []


def test_priority_order_and_victim_choice():
    m = mgr()
    m.request("S1", "s1", P.SHADOW, 0.5, NOW)
    m.request("P1", "p1", P.PRODUCTION, 0.9, NOW)
    m.request("S2", "s2", P.SHADOW, 0.9, NOW)
    # victim: lowest priority, then least recently requested -> S1 (older) even with lower conf
    assert m.request("P2", "p2", P.PRODUCTION, 0.7, NOW)
    assert m.drain_evicted() == [("s1", "S1")] and "S1" not in m.desired()
    # position outranks everything; evicts remaining shadow first
    assert m.request("POS", "pos", P.POSITION, 1.0, NOW)
    assert m.drain_evicted() == [("s2", "S2")]
    assert m.request("P3", "p3", P.POSITION, 1.0, NOW)  # evicts a production (LRU: P1)
    assert m.drain_evicted() == [("p1", "P1")]
    assert m.drain_evicted() == []


def test_lower_confidence_breaks_recency_tie_via_rank():
    m = mgr(cap=4)
    m.request("A", "a", P.SHADOW, 0.9, NOW)
    m.request("B", "b", P.SHADOW, 0.1, NOW)
    m._holds["A"]["a"].seq = m._holds["B"]["b"].seq = 1  # force recency tie
    assert m.request("C", "c", P.PRODUCTION, 0.5, NOW)
    assert m.drain_evicted() == [("b", "B")]


def test_position_never_evicted_and_reserved_never_evicted():
    m = mgr()
    for s in ("A", "B", "C"):
        m.request(s, s, P.POSITION, 1.0, NOW)
    assert not m.request("D", "d", P.POSITION, 1.0, NOW)
    assert m.desired() >= {"SPY", "QQQ", "A", "B", "C"} and m.drain_evicted() == []


def test_reserved_request_takes_no_extra_slot():
    m = mgr()
    assert m.request("SPY", "x", P.SHADOW, 0.5, NOW)
    for s in ("A", "B", "C"):
        assert m.request(s, s, P.PRODUCTION, 0.8, NOW)
    m.release("x")
    assert "SPY" in m.desired()


def test_shadow_cap():
    m = mgr(cap=30, shadow_max=2)
    assert m.request("A", "a", P.SHADOW, 0.5, NOW) and m.request("B", "b", P.SHADOW, 0.5, NOW)
    assert not m.request("C", "c", P.SHADOW, 0.5, NOW)
    assert m.request("C", "c", P.PRODUCTION, 0.5, NOW)  # production is not capped by shadow_max
    m.promote("a", P.PRODUCTION)
    assert m.request("D", "d", P.SHADOW, 0.5, NOW)  # shadow count dropped to 1
    assert m.request("A", "a2", P.SHADOW, 0.5, NOW)  # existing symbol needs no slot


def test_refcount_two_owners_same_symbol():
    m = mgr()
    m.request("A", "o1", P.SHADOW, 0.5, NOW)
    m.request("A", "o2", P.PRODUCTION, 0.6, NOW)
    assert m.priority("A") == P.PRODUCTION
    m.release("o2")
    assert "A" in m.desired() and m.priority("A") == P.SHADOW
    m.release("o1")
    assert "A" not in m.desired()


def test_promote_protects_from_eviction_and_evicts_all_owners():
    m = mgr()
    m.request("A", "o1", P.SHADOW, 0.5, NOW)
    m.request("A", "o2", P.SHADOW, 0.5, NOW)
    m.request("B", "b", P.SHADOW, 0.5, NOW)
    m.request("C", "c", P.SHADOW, 0.5, NOW)
    m.promote("b", P.POSITION)
    m.promote("c", P.POSITION)
    assert m.request("D", "d", P.PRODUCTION, 0.9, NOW)
    assert sorted(m.drain_evicted()) == [("o1", "A"), ("o2", "A")]
