"""Unit tests for event bus."""

from marketpipe.events import EventBus, DomainEvent


class X(DomainEvent):
    pass


def test_event_subscription_and_publishing():
    """Test event subscription and publishing."""
    hit = []
    EventBus.subscribe(X, lambda e: hit.append(1))
    EventBus.publish(X())
    assert hit == [1]


def test_multiple_subscribers():
    """Test multiple subscribers for same event type."""
    results = []
    
    def handler1(event):
        results.append("handler1")
    
    def handler2(event):
        results.append("handler2")
    
    EventBus.subscribe(X, handler1)
    EventBus.subscribe(X, handler2)
    EventBus.publish(X())
    
    assert "handler1" in results
    assert "handler2" in results 